import unittest
import time

from unittest import TestCase
from network import Network, Node

class NetworkTest(TestCase):

	def setUp(self):
		self.nodes = []

	def tearDown(self):
		for node in self.nodes:
			node.socket.close()

	def test_network_nodes_are_correctly_parsed(self):
		node_ids = [1, 2]
		node_addresses = [
			('127.0.0.1', 5001), ('127.0.0.1', 5002)
		]
		network = Network(node_ids, node_addresses)

		node1 = Node(network, 1, ('127.0.0.1', 5001))
		node2 = Node(network, 2, ('127.0.0.1', 5002))

		self.assertEqual(network.nodes, 
			{
				1: ('127.0.0.1', 5001), 
				2: ('127.0.0.1', 5002)
			})

		self.nodes.extend([node1, node2])

	def test_node_broadcast_through_network(self):
		node_ids = [3, 4]
		node_addresses = [
			('127.0.0.1', 5003), ('127.0.0.1', 5004)
		]
		network = Network(node_ids, node_addresses)

		node1 = Node(network, 3, ('127.0.0.1', 5003))
		node2 = Node(network, 4, ('127.0.0.1', 5004))
		node1.start_listening()
		node2.start_listening()
		node1.broadcast("Hello from node3")

		time.sleep(0.1)

		self.assertEqual(node2.last_message, "Hello from node3")
		self.nodes.extend([node1, node2])

	def test_node_broadcast_multiple_message_through_network(self):
		node_ids = [5, 6]
		node_addresses = [
			('127.0.0.1', 5005), ('127.0.0.1', 5006)
		]
		network = Network(node_ids, node_addresses)

		node1 = Node(network, 5, ('127.0.0.1', 5005))
		node2 = Node(network, 6, ('127.0.0.1', 5006))
		node1.start_listening()
		node2.start_listening()
		node1.broadcast("Hello from node5")

		time.sleep(0.1)

		self.assertEqual(node2.last_message, "Hello from node5")
		self.nodes.extend([node1, node2])	

		node2.broadcast("Reply from node6")
		time.sleep(0.1)
		self.assertEqual(node1.last_message, "Reply from node6")

		node1.broadcast("Hello again from node5")
		time.sleep(0.1)
		self.assertEqual(node2.last_message, "Hello again from node5")

	def test_node_relay_through_network(self):
		node_ids = [7, 8, 9]
		node_addresses = [
			('127.0.0.1', 5007), ('127.0.0.1', 5008), 
			('127.0.0.1', 5009) 
		]
		network = Network(node_ids, node_addresses)

		node1 = Node(network, 7, ('127.0.0.1', 5007))
		node2 = Node(network, 8, ('127.0.0.1', 5008))
		node3 = Node(network, 9, ('127.0.0.1', 5009))
		node1.start_listening()
		node2.start_listening()
		node3.start_listening()
		node1.send_to(9, "Hello from node7")

		time.sleep(0.1)

		self.assertEqual(node2.last_message, None)
		self.assertEqual(node3.last_message, "Hello from node7")
		self.nodes.extend([node1, node2, node3])
unittest.main()