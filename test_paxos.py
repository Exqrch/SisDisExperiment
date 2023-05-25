import unittest
import time

from unittest import TestCase
from network import Network
from paxos import PaxosNode

class PaxosTest(TestCase):

	def setUp(self):
		self.nodes = []

	def tearDown(self):
		for node in self.nodes:
			node.socket.close()

	def test_leader_protocol(self):
		node_ids = [1, 2, 3, 4, 5]
		node_addresses = [
			('127.0.0.1', 5001), ('127.0.0.1', 5002), 
			('127.0.0.1', 5003), ('127.0.0.1', 5004), ('127.0.0.1', 5005)
		]
		network = Network(node_ids, node_addresses)

		q_path = 'queries/best/paxos'

		node1 = PaxosNode(network, 1, ('127.0.0.1', 5001), q_path)
		node2 = PaxosNode(network, 2, ('127.0.0.1', 5002), q_path)
		node3 = PaxosNode(network, 3, ('127.0.0.1', 5003), q_path)
		node4 = PaxosNode(network, 4, ('127.0.0.1', 5004), q_path)
		node5 = PaxosNode(network, 5, ('127.0.0.1', 5005), q_path)

		node1.start_listening()
		node2.start_listening()
		node3.start_listening()
		node4.start_listening()
		node5.start_listening()

		node1.leader_protocol_thread.start()
		node2.leader_protocol_thread.start()
		node3.leader_protocol_thread.start()
		node4.leader_protocol_thread.start()
		node5.leader_protocol_thread.start()

		time.sleep(10)

		# self.assertEqual(node1.is_leader, True)
		# self.assertEqual(node2.is_leader, False)
		# self.assertEqual(node3.is_leader, False)

		# self.assertEqual(node1.leader_id, 1)
		# self.assertEqual(node2.leader_id, 1)
		# self.assertEqual(node3.leader_id, 1)
		
		self.nodes.extend([node1, node2, node3, node4, node5])


unittest.main()