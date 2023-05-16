import queue
import math
import socket
import threading
import pickle
import os

import time
import sys
from network import Node

# Multi-Paxos Node, using a variation of single-degree paxos.
# Every node must have a log of their own.
class PaxosNode(Node):
	# Algorithm is adapted from: "https://www.youtube.com/watch?v=SRsK-ZXTeZ0"

	def __init__(self, network, node_id, node_address):
		super().__init__(network, node_id, node_address)

		# Time-keeping
		self.start_time = time.monotonic()
		self.query_time = []
		self.end_time = None

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

		self.finished_node = set()

		# acceptor only
		self.lock = False
		self.ongoing_p_id = None
		self.already_request_proposal_id = False

		# DEBUG
		self.still_alive_thread = threading.Thread(target=self.still_alive)

	# DEBUG #
	def still_alive(self):
		while True:
			print(f"Node {self.node_id} is alive")
			time.sleep(5)
	# DEBUG #
		

	# Set leader using bully protocol: Smallest ID win
	# =================================================
	def start_leader_protocol(self):
		# print(f"Node {self.node_id} start leader election")
		self.broadcast(f"ELECTION-{self.node_id}")

	def leader_protocol(self, recv_node_id):
		# print(f"Node {self.node_id} enters leader_protocol")
		self.heard.add(recv_node_id)
		# print(f"Node {self.node_id} has heard from {self.heard}")

		if len(self.heard) == self.node_count:
			candidates = list(self.heard)
			self.leader_id = min(candidates)
			self.heard = set()

			if (self.leader_id == self.node_id):
				# print(f"Node {self.node_id} is now leader")
				self.is_leader = True
			else:
				if not self.already_request_proposal_id:
					# print(f"Node {self.node_id} is now request proposal id")
					self.already_request_proposal_id = True
					threading.Thread(target=self.request_proposal_id).start()
	# =================================================

	# Consensus Phase
	# =================================================
	# Leader only
	def generate_proposal_id(self):
		if self.is_leader:
			self.proposal_id += 1
			return self.proposal_id
		return f"Node {self.node_id} is Unallowed to generate proposal id"

	# When a proposer wants to create a proposal, 
	# it asks the leader for the appropriate proposal_id
	# the leader locks this function if the proposal with proposal_id is not yet accepted.
	def serve_request_proposal_id(self, requester_node_id):
		# REQUEST_PID-[requester_node_id]
		if self.is_leader:
			if not self.proposal_id_lock:
				p_id = self.generate_proposal_id()
				self.proposal_id_lock = True
				self.proposal_id_lock_value = p_id
				self.send_to(requester_node_id, f"REPLY_PID-{p_id}")
			else:
				self.send_to(requester_node_id, f"RUNNING_PROPOSAL-{self.proposal_id}")

	def stop_requesting_pid(self, ongoing_p_id):
		# print(f"Node {self.node_id} lock request pid")
		self.lock = True
		self.ongoing_p_id = ongoing_p_id

	def leader_query_run(self):
		if not self.query_not_done():
			self.finished_node.add(self.node_id)
			return 0
		# print(f"Leader node runs query")
		p_id = self.generate_proposal_id()
		self.proposal_id_lock = True
		self.proposal_id_lock_value = p_id
		self.run_query(p_id)

	def run_query(self, given_p_id):
		# print(f"Node {self.node_id} start run_query")
		with open(f'queries/query_{self.node_id}.txt', 'r') as f:
			for i, message in enumerate(f.readlines()):
				if i == self.q_num:
					message = message.strip()
					# print(f"MESSAGE FROM RUN QUERY = {message}")
					if message == "":
						break
					self.q_num += 1
					if self.is_leader:
						self.current_proposal_order = message
						self.prepare_protocol(self.proposal_id_lock_value)
					else:
						# print(f"Node {self.node_id} sends to leader {message}")
						self.send_to(self.leader_id, f"{message}-{given_p_id}")
					break
	# Leader only
	def prepare_protocol(self, proposal_id):
		# print(f"Node {self.node_id} start prepare protocol")
		self.broadcast(f"PREPARE-{proposal_id}")

	def promise_protocol(self, proposal_id):
		# print(f"Node {self.node_id} response with promise")
		self.send_to(self.leader_id, f"PROMISE-{self.node_id}-{proposal_id}")

	# PROMISE-[node_id]-[p_id]
	def serve_promise_protocol(self, message):
		node_id = int(message.split('-')[1])
		proposal_id = int(message.split('-')[2])
		# print(f"Received message from {node_id} = {message}, proposal_id = {proposal_id}")

		if proposal_id == self.proposal_id_lock_value:
			# print(f"Node {self.node_id} success check id_lock_value")
			self.promised_nodes.add(node_id)
			if len(self.promised_nodes) == self.node_count-1:
				self.promised_nodes = set()
				# print(f"Node {self.node_id} heard PROMISE from all node")
				self.accept_protocol()

	def accept_protocol(self):
		# print(f"Node {self.node_id} starts accept protocol")
		self.broadcast(f"ACCEPT-{self.proposal_id_lock_value}-{self.current_proposal_order}")

	def accepted_protocol(self, order):

		# print(f"Node {self.node_id} accepted the broadcast")
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

		if "SET" in order:
			# ACCEPT-1-SET-b=97
			# print(f"Node {self.node_id} process SET with order = {order}")
			# print(f"processed order = {order.split('-')[-1].split('=')}")
			var_name = order.split('-')[-1].split('=')[0]
			var_value = order.split('-')[-1].split('=')[1]
			memory[var_name] = int(var_value)

			with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
				pickle.dump(memory, f)
			self.send_to(self.leader_id, f"ACCEPTED-{self.ongoing_p_id}-SET-{var_name}={var_value}-{self.node_id}")
		
		elif "READ" in order:
			# print(f"Node {self.node_id} process READ with order = {order}")
			var_name = order.split('-')[-1]
			value = memory.get(var_name, -1)
			if value == -1:
				value = "NOT_FOUND"
			self.send_to(self.leader_id, f"ACCEPTED-{self.ongoing_p_id}-READ-{var_name}={value}-{self.node_id}")


	def serve_accepted_protocol(self, order):
		# print(f"Node {self.node_id} serve accepted protocl")
		# print(f"serve_accepted_protocol order = {order}")
		order = order.split('-')

		node_id = order[-1]
		self.accepted_nodes.add(node_id)
		# print(f"currently nodes that have accepted are = {self.accepted_nodes}")


		if "READ" in self.current_proposal_order:
			# print(f"Node {self.node_id} serve READ with order {order}")
			try:
				self.read_vote[order[3].split('=')[-1]] += 1
			except:
				self.read_vote[order[3].split('=')[-1]] = 1

		if len(self.accepted_nodes) == self.node_count-1:
			# leader's turn to read its own data
			directory = "paxos_disk"
			# print(f"Node {self.node_id} leader read its own data for read and set")
			memory = {}
			if os.path.isfile(f"{directory}/disk_{self.node_id}.pickle"):
				with open(f"{directory}/disk_{self.node_id}.pickle", 'rb') as f:
					memory = pickle.load(f)
			else:
				with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
					pickle.dump(memory, f)
			
			# print(f"leader's current proposal order = {self.current_proposal_order}")
			if "READ" in self.current_proposal_order:
				value = memory.get(self.current_proposal_order.split('-')[-1], -1)
				if value == -1:
					value = 'NOT_FOUND'
				try:
					self.read_vote[value] += 1
				except:
					self.read_vote[value] = 1

				# print(f"LEADER HAS FINISHED VOTE READ PROCESS, RESULT = {max(self.read_vote, key=self.read_vote.get)}")
			elif "SET" in self.current_proposal_order:
				# print(f"current proposal order = {self.current_proposal_order.split('-')}")
				var_name = self.current_proposal_order.split('-')[-1].split('=')[0]
				var_value = self.current_proposal_order.split('-')[-1].split('=')[1]
				memory[var_name] = int(var_value)
				with open(f"{directory}/disk_{self.node_id}.pickle", 'wb') as f:
					pickle.dump(memory, f)


			self.accepted_nodes = set()
			self.start_new_round_protocol()

	def start_new_round_protocol(self):
		# print(f"Node {self.node_id} start new round")

		# print("DEBUG")
		# memory = {}
		# directory = 'paxos_disk'
		# if os.path.isfile(f"{directory}/disk_{self.node_id}.pickle"):
		# 	with open(f"{directory}/disk_{self.node_id}.pickle", 'rb') as f:
		# 		memory = pickle.load(f)
		# 		print(f"the memory of node {self.node_id}\n{memory}\n\n")

		# print(f"QUERY DONE BY {self.node_id} = {self.q_num}")
		self.query_time.append(time.monotonic())
		if self.is_leader:
			# print(f"ROUND {self.proposal_id_lock_value} IS DONE, proposal = {self.current_proposal_order}")
			self.proposal_id_lock = False
			self.proposal_id_lock_value = None
			self.current_proposal_order = None
			self.broadcast(f"NEW_ROUND")
			self.leader_query_run()
		else:
			self.lock = False
			self.ongoing_p_id = None
			self.already_request_proposal_id = False
			self.request_proposal_id()
	# =================================================

	# Multi-Paxos, continous running thread:
	# 1. listen thread (already active)
	# 2. Request proposal_id, gated by self.lock 
	# =================================================
	def request_proposal_id(self):
		if not self.query_not_done():
			self.send_to(self.leader_id, f"DONE-{self.node_id}")
			self.lock = True
			return 0

		if not self.is_leader:
			if self.leader_id is not None and not self.lock:
				self.send_to(self.leader_id, f"REQUEST_PID-{self.node_id}")
	# =================================================

	# UTILS
	# =================================================
	def query_not_done(self):
		with open(f'queries/query_{self.node_id}.txt', 'r') as f:
			for i, message in enumerate(f.readlines()):
				if i == self.q_num:
					message = message.strip()
					# print(f"===\nNode {self.node_id}'s next message = {message}\n===")
					if message == '':
						return False
					return True
			return False

	def done_protocol(self, sender_id):
		self.finished_node.add(sender_id)
		if len(self.finished_node) == self.node_count:
			self.exit_protocol()

	def exit_protocol(self):
		if self.is_leader:
			self.broadcast(f"EXIT")

		# memory = {}
		# directory = 'paxos_disk'
		# if os.path.isfile(f"{directory}/disk_{self.node_id}.pickle"):
		# 	with open(f"{directory}/disk_{self.node_id}.pickle", 'rb') as f:
		# 		memory = pickle.load(f)

		# sorted_memory = dict(sorted(memory.items()))
		# self.memory = sorted_memory
		# print(f"Node {self.node_id} Memories:\n{sorted_memory}\n")

		self.end_time = time.monotonic()

		# Write timing reports
		report = ""
		gap_time = []
		gap_time.append(self.query_time[0] - self.start_time)
		for i in range(1, len(self.query_time)):
			gap_time.append(self.query_time[i] - self.query_time[i-1])

		writef = f"paxos_report/time_{self.node_id}.txt"
		readf  = f"queries/query_{self.node_id}.txt"

		with open(writef, 'w') as f, open(readf, 'r') as g :
			report += f"Start time: {self.start_time}\n"
			report += f"End time  : {self.end_time} \n"
			report += f"Run time  : {self.end_time - self.start_time} \n"
			i = 0
			for line in g.readlines():
				line = line.strip()
				if line != '' or line != ' ':
					report += f"Query {line} took: {gap_time[i]} \n"
					i += 1

			f.write(report)


		sys.exit()
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
				# print(f"{self.node_id} --> OSError OCCURED!")
				order = None
				break

			# ELECTION-[sender_node_id]
			if "EXIT" in order:
				self.exit_protocol()

			if "ELECTION" in order:
				if self.leader_id == None:
					# print(F"Node {self.node_id} hearing election: {order}")
					sender_node_id = int(order.split('-')[1])
					self.leader_protocol(sender_node_id)

			if "REQUEST_PID" in order:
				# print(F"Node {self.node_id} hearing reqest_pid: {order}")
				sender_node_id = int(order.split('-')[1])
				self.serve_request_proposal_id(sender_node_id)

			if "RUNNING_PROPOSAL" in order:
				# print(f"Node {self.node_id} hearing running_proposal: {order}")
				ongoing_p_id = int(order.split('-')[1])
				self.stop_requesting_pid(ongoing_p_id)

			if "REPLY_PID" in order:
				# print(f"Node {self.node_id} hearing reply_pid: {order}")
				given_p_id = int(order.split('-')[1])
				self.stop_requesting_pid(given_p_id)
				self.run_query(given_p_id)

			if "PREPARE" in order:
				# print(f"Node {self.node_id} hearing prepare: {order}")
				p_id = int(order.split('-')[1])
				self.promise_protocol(p_id)

			if "ACCEPT" in order and "ACCEPTED" not in order:
				# print(f"Node {self.node_id} hearing accept: {order}")
				self.accepted_protocol(order)

			if "NEW_ROUND" in order:
				# print(f"Node {self.node_id} hearing new_round: {order}")
				self.start_new_round_protocol()

			if self.is_leader:
				# print(f"order received by leader = {order}")
				if "DONE" in order:
					sender_id = int(order.split('-')[-1])
					self.done_protocol(sender_id)

				if "ACCEPTED" in order:
					# print(f"Node {self.node_id} hearing accepted: {order}")
					self.serve_accepted_protocol(order)
				elif "SET" in order or "READ" in order:
					# print(f"Node {self.node_id} hearing set/read: {order}")
					# SET-b=97-1
					temp = order.split('-')
					p_id = int(temp[-1])
					# print(f"Leader's id_lock = {self.proposal_id_lock_value}, p_id = {p_id}")
					if p_id == self.proposal_id_lock_value:
						self.current_proposal_order = '-'.join(temp[:-1])
						# print(f"Node proposal order = {self.current_proposal_order}")
						# print(f"Node {self.node_id} begins prepare protocol")
						self.prepare_protocol(self.proposal_id_lock_value)
				elif "PROMISE" in order:
					# print(F"Node {self.node_id} hearing promise: {order}")
					self.serve_promise_protocol(order)








