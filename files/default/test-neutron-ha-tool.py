import datetime
import unittest
import collections
import importlib
import logging
ha_tool = importlib.import_module("neutron-ha-tool")


class MockNeutronClient(object):

    def __init__(self):
        self.routers = {}
        self.agents = {}
        self.routers_by_agent = collections.defaultdict(set)

    def tst_add_agent(self, agent_id, props):
        self.agents[agent_id] = dict(props, id=agent_id)

    def tst_add_router(self, agent_id, router_id, props):
        self.routers[router_id] = dict(props, id=router_id)
        self.routers_by_agent[agent_id].add(router_id)

    def tst_agent_by_router(self, router_id):
        for agent_id, router_ids in self.routers_by_agent.items():
            if router_id in router_ids:
                return self.agents[agent_id]

        raise NotImplementedError()

    def list_agents(self):
        return {
            'agents': self.agents.values()
        }

    def list_routers_on_l3_agent(self, agent_id):
        return {
            'routers': [
                self.routers[router_id]
                for router_id in self.routers_by_agent[agent_id]
            ]
        }

    def remove_router_from_l3_agent(self, agent_id, router_id):
        self.routers_by_agent[agent_id].remove(router_id)

    def add_router_to_l3_agent(self, agent_id, router_body):
        self.routers_by_agent[agent_id].add(router_body['router_id'])

    def list_ports(self, device_id, fields):
        return {
            'ports': [
                {
                    'id': 'someid',
                    'binding:host_id':
                        self.tst_agent_by_router(device_id)['host'],
                    'binding:vif_type': 'non distributed',
                    'status': 'ACTIVE'
                }
            ]
        }

    def list_floatingips(self, router_id):
        return {
            'floatingips': [
                {
                    'id': 'irrelevant',
                    'status': 'ACTIVE'
                }
            ]
        }


def make_neturon_client(live_agents=0, dead_agents=0):
    neutron_client = MockNeutronClient()

    for i in range(live_agents):
        neutron_client.tst_add_agent(
            'live-agent-{}'.format(i), {
                'agent_type': 'L3 agent',
                'alive': True,
                'admin_state_up': True,
                'host': 'live-agent-{}-host'.format(i),
                'configurations': {
                    'agent_mode': 'Mode X'
                }
            }
        )
    for i in range(dead_agents):
        neutron_client.tst_add_agent(
            'dead-agent-{}'.format(i), {
                'agent_type': 'L3 agent',
                'alive': False,
                'admin_state_up': False,
                'host': 'dead-agent-{}-host'.format(i)
            }
        )
    return neutron_client


class TestL3AgentMigrate(unittest.TestCase):

    def test_no_dead_agents_returns_zero(self):
        neutron_client = make_neturon_client(live_agents=2)

        result = ha_tool.l3_agent_migrate(neutron_client)

        self.assertEqual(0, result)

    def test_no_alive_agents_returns_one(self):
        neutron_client = make_neturon_client(dead_agents=2)

        result = ha_tool.l3_agent_migrate(neutron_client)

        self.assertEqual(1, result)

    def test_router_moved(self):
        neutron_client = make_neturon_client(live_agents=1, dead_agents=1)
        neutron_client.tst_add_router('dead-agent-0', 'router-1', {})

        result = ha_tool.l3_agent_migrate(neutron_client, now=True)

        self.assertEqual(0, result)
        self.assertEqual(
            set(['router-1']), neutron_client.routers_by_agent['live-agent-0'])


class TestL3AgentEvacuate(unittest.TestCase):

    def test_no_agents_returns_zero(self):
        neutron_client = MockNeutronClient()
        result = ha_tool.l3_agent_evacuate(neutron_client, 'host1')

        self.assertEqual(0, result)

    def test_evacuation(self):
        neutron_client = make_neturon_client(live_agents=2)
        neutron_client.tst_add_router('live-agent-0', 'router', {})

        result = ha_tool.l3_agent_evacuate(neutron_client, 'live-agent-0-host')

        self.assertEqual(0, result)
        self.assertEqual(
            set(['router']),
            neutron_client.routers_by_agent['live-agent-1']
        )


class TestLeastBusyAgentPicker(unittest.TestCase):

    def setUp(self):
        neutron_client = make_neturon_client(live_agents=2)
        self.neutron_client = neutron_client

    def make_picker(self):
        return ha_tool.LeastBusyAgentPicker(
            self.neutron_client,
            [
                {'id': 'live-agent-0'},
                {'id': 'live-agent-1'}
            ]
        )

    def test_initial_numbers_queried(self):
        self.neutron_client.tst_add_router('live-agent-0', 'router', {})
        picker = self.make_picker()

        self.assertEqual(
            {
                'live-agent-0': 1,
                'live-agent-1': 0
            },
            picker.router_count_per_agent_id
        )

    def test_least_busy_picked(self):
        self.neutron_client.tst_add_router('live-agent-0', 'router', {})
        picker = self.make_picker()

        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_router_counts_maintained(self):
        self.neutron_client.tst_add_router('live-agent-0', 'router', {})
        picker = self.make_picker()

        picked_agent = picker.pick()
        self.assertEqual('live-agent-1', picked_agent['id'])

        self.assertEqual(
            {
                'live-agent-0': 1,
                'live-agent-1': 1
            },
            picker.router_count_per_agent_id
        )

    def test_routers_picked_evenly(self):
        picker = self.make_picker()

        self.assertEqual('live-agent-0', picker.pick()['id'])
        self.assertEqual('live-agent-1', picker.pick()['id'])
        self.assertEqual('live-agent-0', picker.pick()['id'])

    def test_cache_reloaded(self):
        picker = self.make_picker()  # This makes the initial query to neutron

        # Add some routers to live-agent-0 to make sure it's the busyest
        self.neutron_client.tst_add_router('live-agent-0', 'router-2', {})
        self.neutron_client.tst_add_router('live-agent-0', 'router-3', {})

        # Emulate that cache has expired
        picker.cache_created_at = (
            picker.cache_created_at - datetime.timedelta(
                seconds=ha_tool.ROUTER_CACHE_MAX_AGE_SECONDS + 1)
        )

        # pick returns live-agent-1 - that means it consulted neutron
        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_cache_reloaded_if_difference_is_a_day(self):
        picker = self.make_picker()  # This makes the initial query to neutron

        # Add some routers to live-agent-0 to make sure it's the busyest
        self.neutron_client.tst_add_router('live-agent-0', 'router-2', {})
        self.neutron_client.tst_add_router('live-agent-0', 'router-3', {})

        # Emulate that cache has expired
        picker.cache_created_at = (
            picker.cache_created_at - datetime.timedelta(days=1)
        )

        # pick returns live-agent-1 - that means it consulted neutron
        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_pick_on_empty_array_throws_index_error_as_random_does(self):
        picker = ha_tool.LeastBusyAgentPicker(self.neutron_client, [])

        with self.assertRaises(IndexError):
            picker.pick()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
