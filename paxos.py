import queue
import math
import socket
import threading
import pickle
import os

from network import Node

# Multi-Paxos Node, using a variation of single-degree paxos.
# Every node must have a log of their own.
class PaxosNode(Node):
	# Algorithm is adapted from: "https://www.youtube.com/watch?v=SRsK-ZXTeZ0"

	def __init__(self, network, node_id, node_address):
		super().__init__(network, node_id, node_address)

		# Phase 0: Leader selection through bully protocol
		self.leader_id = None
		self.is_leader = False
		self.leader_protocol_thread = threading.Thread(target=self.start_leader_protocol)
		self.node_count = len(self.network.nodes)
		self.heard = set()
		self.heard.add(self.node_id)

		# Phase 1: Prepare Proposal ID
		self.q_num = 0

		# leader only
		self.proposal_id = 0
		self.proposal_id_lock = False
		self.proposal_id_lock_value = None

		self.current_proposal_order = None

		self.promised_nodes = set()

		self.read_vote = {}
		self.accepted_nodes = set()

		# acceptor only
		self.lock = False
		self.ongoing_p_id = None
		

	# Set leader using bully protocol: Smallest ID win
	# =================================================
	def start_leader_protocol(self):
		self.broadcast(f"ELECTION-{self.node_id}")

	def leader_protocol(self, recv_node_id):
		self.heard.add(recv_node_id)

		if len(self.heard) == self.node_count:
			candidates = list(self.heard)
			self.leader_id = min(candidates)
			self.heard = set()

			if (self.leader_id == self.node_id):
				self.is_leader = True
			else:
				threading.Thread(target=self.request_proposal_id).start()
	# =================================================

	# Consensus Phase
	# =================================================
	# Leader only
	def generate_proposal_id(self):
		if self.is_leader:
			self.proposal_id += 1
			return self.proposal_id
		return -1

	# When a proposer wants to create a proposal, 
	# it asks the leader for the appropriate proposal_id
	# the leader locks this function if the proposal with proposal_id is not yet accepted.
	def serve_request_proposal_id(self, requester_node_id):
		# REQUEST_PID-[requester_node_id]
		if self.is_leader:
			while True:
				if not self.proposal_id_lock:
					p_id = self.generate_proposal_id()
					self.send_to(requester_node_id, f"REPLY_PID-{p_id}")
					break
				else:
					self.send_to(requester_node_id, f"RUNNING_PROPOSAL-{self.proposal_id}")

	def request_proposal_id(self):
		if not self.lock_request:
			self.send_to(self.leader_id, f"REQUEST_PID-{self.node_id}")

	def stop_requesting_pid(self, ongoing_p_id):
		self.lock_request = True
		self.ongoing_p_id = ongoing_p_id

	def run_query(self, given_p_id):
		with open(f'queries/query_{self.node_id}', 'r') as f:
			for i, message in enumerate(f.readlines()):
				if i == self.q_num:
					self.send_to(self.leader_id, f"{message}-{given_p_id}")

	# Leader only
	def prepare_protocol(self, proposal_id):
		self.broadcast(f"PREPARE-{proposal_id}")

	def promise_protocol(self, proposal_id):
		self.send_to(self.leader_id, f"PROMISE-{proposal_id}")

	# PROMISE-[node_id]-[p_id]
	def serve_promise_protocol(self, message):
		node_id = int(message.split('-')[1])
		proposal_id = int(message.split('-')[2])

		if proposal_id == self.proposal_id_lock_value:
			self.promised_nodes.add(node_id)
			if len(self.promised_nodes) == self.node_count-1:
				self.promised_nodes = set()
				self.accept_protocol()

	def accept_protocol(self):
		self.broadcast(f"ACCEPT-{self.proposal_id_lock_value}-{self.current_proposal_order}")

	def accepted_protocol(self, order):
		directory = "paxos_disk"
		if not os.path.exists(directory):
			os.makedirs(directory)

		memory = {}
		if os.path.isfile(f"{directory}/disk_{self.node_id}.pickle"):
			with open(f"{directory}/disk_{self.node_id}.pickle", 'rb') as f:
				memory = pickle.load(f)
		else:
			with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
				pickle.dump(memory, f)

		# Message could be:
		# 1. SET-[variable_name]=[value]
		# 2. READ-[variable_name]

		if "SET" in order:
			var_name = order.split('-')[1].split('=')[0]
			var_value = order.split('-')[1].split('=')[1]
			memory[var_name] = int(var_value)

			with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
				pickle.dump(memory, f)
			self.send_to(self.leader_id, f"ACCEPTED-{self.ongoing_p_id}-{order}-{self.node_id}")
		
		elif "READ" in order:
			var_name = order.split('-')[1]
			value = memory.get(var_name, -1)
			self.send_to(self.leader_id, f"ACCEPTED-{self.ongoing_p_id}-{order}-{self.node_id}-{value}")


	def serve_accepted_protocol(self, order):
		order = order.split('-')

		node_id = order[4]
		self.accepted_nodes.add(node_id)

		if "READ" in self.current_proposal_order:
			try:
				self.read_vote[int(order[5])] += 1
			except:
				self.read_vote[int(order[5])] = 1

		if len(self.accepted_nodes) == self.node_count-1:
			# leader's turn to read its own data

			memory = {}
			if os.path.isfile(f"{directory}/disk_{self.node_id}.pickle"):
				with open(f"{directory}/disk_{self.node_id}.pickle", 'rb') as f:
					memory = pickle.load(f)
			else:
				with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
					pickle.dump(memory, f)
			
			if "READ" in self.current_proposal_order:
				value = memory.get(order[3], -1)
				try:
					self.read_vote[value] += 1
				except:
					self.read_vote[value] = 1
			elif "SET" in self.current_proposal_order:
				var_name = order[4].split('=')[0]
				var_value = order[4].split('=')[1]
				memory[var_name] = var_value
				with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
					pickle.dump(memory, f)

			self.accepted_nodes = set()
			self.start_new_round_protocol()

	def start_new_round_protocol(self):
		if self.is_leader:
			self.proposal_id_lock = False
			self.proposal_id_lock_value = None
			self.current_proposal_order = None

			self.broadcast(f"NEW_ROUND")
	# =================================================

	# Multi-Paxos, continous running thread:
	# 1. listen thread (already active)
	# 2. Request proposal_id, gated by self.lock 
	# =================================================
	def request_proposal_id(self):
		if not self.is_leader:
			while True:
				if self.lock:
					continue

				if self.leader_id is not None:
					self.send_to(self.leader_id, f"REQUEST_PID-{self.node_id}")
	# =================================================

	def listen(self):
		self.socket.listen()
		while True:
			try:
				conn, addr = self.socket.accept()
				data = conn.recv(1024)
				order = data.decode('utf-8')
				self.last_message = order
				conn.close()
			except OSError:
				order = None
				break

			# ELECTION-[sender_node_id]
			if "ELECTION" in order:
				sender_node_id = int(order.split('-')[1])
				self.leader_protocol(sender_node_id)

			if "REQUEST_PID" in order:
				sender_node_id = int(order.split('-')[1])
				self.serve_request_proposal_id(sender_node_id)

			if "RUNNING_PROPOSAL" in order:
				ongoing_p_id = int(order.split('-')[1])
				self.stop_requesting_pid(ongoing_p_id)

			if "REPLY_PID" in order:
				given_p_id = int(order.split('-')[1])
				self.stop_requesting_pid(given_p_id)
				self.run_query(given_p_id)

			if "ACCEPT" in order:
				self.accepted_protocol(order)

			if "NEW_ROUND" in order:
				self.ongoing_p_id = None
				self.lock = False

			if self.is_leader:
				if "SET" in order or "READ" in order:
					temp = order.split('-')
					p_id = int(temp[-1])

					if p_id == self.proposal_id_lock_value:
						self.current_proposal_order = ''.join(temp[:-1])
						self.prepare_protocol(self.proposal_id_lock_value)
				elif "PROMISE" in order:
					self.serve_promise_protocol(order)
				elif "ACCEPTED" in order:
					self.serve_accepted_protocol(order)








