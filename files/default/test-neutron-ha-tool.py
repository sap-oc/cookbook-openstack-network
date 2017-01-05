import datetime
import unittest
import collections
import importlib
import logging
ha_tool = importlib.import_module("neutron-ha-tool")


class FakeNeutron(object):
    def __init__(self):
        self.routers = {}
        self.agents = {}
        self.routers_by_agent = collections.defaultdict(set)

    def add_agent(self, agent_id, props):
        self.agents[agent_id] = dict(props, id=agent_id)

    def add_router(self, agent_id, router_id, props):
        self.routers[router_id] = dict(props, id=router_id)
        self.routers_by_agent[agent_id].add(router_id)

    def agent_by_router(self, router_id):
        for agent_id, router_ids in self.routers_by_agent.items():
            if router_id in router_ids:
                return self.agents[agent_id]

        raise NotImplementedError()


class FakeNeutronClient(object):
    def __init__(self, fake_neutron):
        self.fake_neutron = fake_neutron

    def list_agents(self):
        return {
            'agents': self.fake_neutron.agents.values()
        }

    def list_routers_on_l3_agent(self, agent_id):
        return {
            'routers': [
                self.fake_neutron.routers[router_id]
                for router_id in self.fake_neutron.routers_by_agent[agent_id]
            ]
        }

    def remove_router_from_l3_agent(self, agent_id, router_id):
        self.fake_neutron.routers_by_agent[agent_id].remove(router_id)

    def add_router_to_l3_agent(self, agent_id, router_body):
        self.fake_neutron.routers_by_agent[agent_id].add(
            router_body['router_id'])

    def list_ports(self, device_id, fields):
        return {
            'ports': [
                {
                    'id': 'someid',
                    'binding:host_id':
                        self.fake_neutron.agent_by_router(device_id)['host'],
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


def setup_fake_neutron(live_agents=0, dead_agents=0):
    fake_neutron = FakeNeutron()

    for i in range(live_agents):
        fake_neutron.add_agent(
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
        fake_neutron.add_agent(
            'dead-agent-{}'.format(i), {
                'agent_type': 'L3 agent',
                'alive': False,
                'admin_state_up': False,
                'host': 'dead-agent-{}-host'.format(i)
            }
        )
    return fake_neutron


class TestL3AgentMigrate(unittest.TestCase):

    def test_no_dead_agents_migrate_returns_without_errors(self):
        neutron_client = FakeNeutronClient(setup_fake_neutron(live_agents=2))

        # None as Agent Picker - given no dead agents, no migration, and
        # therefore no agent picking will take place
        error_count = ha_tool.l3_agent_migrate(
            neutron_client, None, ha_tool.NullRouterFilter()
        )

        self.assertEqual(0, error_count)

    def test_no_live_agents_migrate_returns_with_error(self):
        neutron_client = FakeNeutronClient(setup_fake_neutron(dead_agents=2))

        # None as Agent Picker - given no live agents, no migration, and
        # therefore no agent picking will take place
        error_count = ha_tool.l3_agent_migrate(
            neutron_client, None, ha_tool.NullRouterFilter()
        )

        self.assertEqual(1, error_count)

    def test_migrate_from_dead_agent_moves_routers_and_returns_no_errors(self):
        fake_neutron = setup_fake_neutron(live_agents=1, dead_agents=1)
        neutron_client = FakeNeutronClient(fake_neutron)
        fake_neutron.add_router('dead-agent-0', 'router-1', {})

        error_count = ha_tool.l3_agent_migrate(
            neutron_client, ha_tool.RandomAgentPicker(),
            ha_tool.NullRouterFilter(), now=True
        )

        self.assertEqual(0, error_count)
        self.assertEqual(
            set(['router-1']), fake_neutron.routers_by_agent['live-agent-0'])


class TestL3AgentEvacuate(unittest.TestCase):

    def test_evacuate_without_agents_returns_no_errors(self):
        neutron_client = FakeNeutronClient(FakeNeutron())

        # None as Agent Picker - given no agents, no migration, and therefore no
        # agent picking will take place
        error_count = ha_tool.l3_agent_evacuate(
            neutron_client, 'host1', None, ha_tool.NullRouterFilter()
        )

        self.assertEqual(0, error_count)

    def test_evacuate_live_agent_moves_routers_and_returns_no_errors(self):
        fake_neutron = setup_fake_neutron(live_agents=2)
        neutron_client = FakeNeutronClient(fake_neutron)
        fake_neutron.add_router('live-agent-0', 'router', {})

        error_count = ha_tool.l3_agent_evacuate(
            neutron_client, 'live-agent-0-host', ha_tool.RandomAgentPicker(),
            ha_tool.NullRouterFilter()
        )

        self.assertEqual(0, error_count)
        self.assertEqual(
            set(['router']),
            fake_neutron.routers_by_agent['live-agent-1']
        )


class TestLeastBusyAgentPicker(unittest.TestCase):

    def setUp(self):
        self.fake_neutron = setup_fake_neutron(live_agents=2)
        self.neutron_client = FakeNeutronClient(self.fake_neutron)

    def make_picker_and_set_agents(self):
        picker = ha_tool.LeastBusyAgentPicker(self.neutron_client)
        picker.set_agents(
            [
                {'id': 'live-agent-0'},
                {'id': 'live-agent-1'}
            ]
        )
        return picker

    def test_agent_picker_queries_neutron_for_number_of_routers(self):
        self.fake_neutron.add_router('live-agent-0', 'router', {})
        picker = self.make_picker_and_set_agents()

        self.assertEqual(
            {
                'live-agent-0': 1,
                'live-agent-1': 0
            },
            picker.router_count_per_agent_id
        )

    def test_agent_with_smallest_number_of_routers_picked(self):
        self.fake_neutron.add_router('live-agent-0', 'router', {})
        picker = self.make_picker_and_set_agents()

        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_picking_an_agent_increases_internal_router_counter_per_agent(self):
        self.fake_neutron.add_router('live-agent-0', 'router', {})
        picker = self.make_picker_and_set_agents()

        picked_agent = picker.pick()
        self.assertEqual('live-agent-1', picked_agent['id'])

        self.assertEqual(
            {
                'live-agent-0': 1,
                'live-agent-1': 1
            },
            picker.router_count_per_agent_id
        )

    def test_router_per_agent_cache_updated_when_cache_expired(self):
        # initializing the picker will also query neutron
        picker = self.make_picker_and_set_agents()

        # Add some routers to live-agent-0 to make sure it's the busyest
        self.fake_neutron.add_router('live-agent-0', 'router-2', {})
        self.fake_neutron.add_router('live-agent-0', 'router-3', {})

        # Emulate that cache has expired
        picker.cache_created_at = (
            picker.cache_created_at - datetime.timedelta(
                seconds=ha_tool.ROUTER_CACHE_MAX_AGE_SECONDS + 1)
        )

        # pick returns live-agent-1 - that means it consulted neutron
        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_cache_reloaded_if_difference_is_a_day(self):
        # initializing the picker will also query neutron
        picker = self.make_picker_and_set_agents()

        # Add some routers to live-agent-0 to make sure it's the busyest
        self.fake_neutron.add_router('live-agent-0', 'router-2', {})
        self.fake_neutron.add_router('live-agent-0', 'router-3', {})

        # Emulate that cache has expired
        picker.cache_created_at = (
            picker.cache_created_at - datetime.timedelta(days=1)
        )

        # pick returns live-agent-1 - that means it consulted neutron
        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_pick_on_empty_array_throws_index_error_as_random_does(self):
        picker = ha_tool.LeastBusyAgentPicker(self.neutron_client)
        picker.set_agents([])

        with self.assertRaises(IndexError):
            picker.pick()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
