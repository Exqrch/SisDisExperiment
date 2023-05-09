from multiprocessing import process
from network import VSRNode
import sys
import os
import time
import random

MAX_NOPS = 10

def operation(i): return lambda state: (state+[i], ['result',i,'on',state])
operations = {i: operation(i) for i in range(MAX_NOPS)}

class Replica(VSRNode):
	def __init__(self, replicas:set, quorumsize:int, tout:float):

		# State variables for the replica
		# We don't need The replica number. This is the index into the con- figuration where this replica’s IP address is stored.

		self.status = "NORMAL"			# The current status, either NORMAL, VIEW_CHANGE, or RECOVERY.
		self.v = 0						# The current view-number, initially 0.
		self.s = 0
		self.n = 0						# The op-number assigned to the most recently received request, initially 0.
		self.k = 0						# The commit-number is the op-number of the most recently committed operation.
		self.logs = list()				# The log. This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
		self.client_table = dict() 		# The client-table. This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
		self.config = list(replicas)	# The configuration. This is a sorted array containing the IP addresses of each of the 2f + 1 replicas.
		self.config.sort()
		replicas = replicas - {self}

		#Timeout and heartbeat Implementation variables
		self.num_req_latest = 0
		self.num_req_obsolete = 0
		self.num_prepare_latest = 0
		self.num_prepare_obsolete = 0
		self.heartbeat = 0
		self.last_commit_num = 0


		# View Change implementation variable
		self.start_view_change_req = set()
		self.last_normal_view = 0

		#Control Variables
		self.done = False

		print("Replica setup completed")


class Client(VSRNode):
	def __init__(self, replicas:Replica, tout, node_address):
		super().__init__(node_address, tout)

		self.state = None												# Current State of the client
		self.config = list(replicas)
		self.config.sort()												# Configuration setting of the current replica group
		self.v = 0														# View Number for the current request
		self.c = self													# Client Id
		self.s  = 0														# Request Number
		self.primary = self.config[self.v]
		self.results = dict()
		self.tout = tout
		print("Client::", self, "setup completed")


		# DS for measuring Performance
		self.perf_data = []
		self.cur_req = 0

	# ========================================================================
	# Client sends request one by one
	# Client Will always try to resend the request to primary if failed
	# Update request number only after the previous request has been completed
	# ========================================================================
	def run(self):
		print("Client:: Running...")
		success = False

		# self.perf_data.append(time.perf_counter())
		curr_op = random.randint(0, MAX_NOPS-1)						# Operation to be carried out

		# Sending request to primary
		while not success:
			success = self.send_to_primary(curr_op)

		print("Client:: Finished sending message to replica")

	def send_to_primary(self, curr_op):
		try:
			print("Client:: Sending request to primary :", self.s, curr_op)
			reply = self.send_to(self.primary, f'REQUEST-{curr_op}-{self.c}-{self.s}')
			print("Client:: Received message :", reply)
			if "REPLY" in reply:
				order, view_number, c_id, result = reply.split("-")
				# tend = time.perf_counter()
				# self.perf_data[cur_req] = tend - self.perf_data[cur_req]
				cur_req = cur_req + 1
				if self.v  != view_number:
					self.v = view_number
					self.primary = self.config[self.v]

				print("Client:: The current view is :", self.v)
				if c_id not in self.results:
					self.results[c_id] = result
				elif self.results[c_id] != result:
					print('Client:: Different result for request', c_id, result, 'than', self.results[c_id])

			if self.s in self.results:
				self.s += 1
				return True

		except TimeoutError:
			print("Client:: Timeout occured on REPLY for op", self.curr_op)
			return False
	# ========================================================================