import unittest
import importlib
import mock
import datetime
from oslo_utils import timeutils


evacuate_lbaas = importlib.import_module("neutron-evacuate-lbaasv2-agent")


class FakeSqlResult():
    def fetchall(self):
        return [
            ['1', 'healthy', timeutils.utcnow()],
            ['2', 'dead', timeutils.utcnow() - datetime.timedelta(seconds=100)]
        ]


class FakeSqlConnection():
    def execute(self, *args):
        return FakeSqlResult()


class TestEvacuateLbaasV2Agents(unittest.TestCase):
    def setUp(self):
        self.evacuate_lbaas = evacuate_lbaas.EvacuateLbaasV2Agent()
        self.evacuate_lbaas.connection = FakeSqlConnection()
        self.evacuate_lbaas.host_to_evacuate = "evacuateme"

    def test_available_agents_exclude_dead_agents(self):
        self.assertEqual(
            [{'host': 'healthy', 'id': '1'}],
            self.evacuate_lbaas.available_destination_agents()
        )

    def test_reassing_single_lb_returns_one_agent(self):
        agents = [
            {'host': 'node1', 'id': '1'},
            {'host': 'node2', 'id': '2'},
            {'host': 'node3', 'id': '3'}
        ]
        loadbalancers = ['abc']

        res = self.evacuate_lbaas.reassign_loadbalancers(loadbalancers, agents)
        self.assertEqual(1, len(res))

    @mock.patch('neutron-evacuate-lbaasv2-agent.'
                'EvacuateLbaasV2Agent.loadbalancers_on_agent')
    @mock.patch('neutron-evacuate-lbaasv2-agent.'
                'RemoteLbaasV2Cleanup')
    def test_restarts_agents_using_crm_on_ha(self, mock_cleanup, mock_lbaas):
        mock_lbaas.return_value = ['lb1']
        evacuate_lbaas.cfg.CONF.set_override("use_crm", True)

        self.evacuate_lbaas.run()
        self.assertEqual(
            mock_cleanup.return_value.restart_lbaasv2_agent_crm.call_count,
            2
        )
        self.assertEqual(
            mock_cleanup.return_value.restart_lbaasv2_agent_systemd.call_count,
            0
        )

    @mock.patch('neutron-evacuate-lbaasv2-agent.'
                'EvacuateLbaasV2Agent.loadbalancers_on_agent')
    @mock.patch('neutron-evacuate-lbaasv2-agent.'
                'RemoteLbaasV2Cleanup')
    def test_restarts_agents_using_systemd_no_ha(self,
                                                 mock_cleanup,
                                                 mock_lbaas):
        mock_lbaas.return_value = ['lb1']
        evacuate_lbaas.cfg.CONF.set_override("use_crm", False)
        self.evacuate_lbaas.run()
        self.assertEqual(
            mock_cleanup.return_value.restart_lbaasv2_agent_crm.call_count,
            0
        )
        self.assertEqual(
            mock_cleanup.return_value.restart_lbaasv2_agent_systemd.call_count,
            2
        )
