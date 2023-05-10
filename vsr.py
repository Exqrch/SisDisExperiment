from multiprocessing import process
from network import AsyncNode
import sys
import os
import time
import random
import pickle
import asyncio

MAX_NOPS = 10

def operation(i): return lambda state: (state+[i], ['result',i,'on',state])
operations = {i: operation(i) for i in range(MAX_NOPS)}

class Replica(AsyncNode):
	def __init__(self, replicas:set, quorumsize:int, tout:float, clients:dict):

		# State variables for the replica
		# We don't need The replica number. This is the index into the configuration where this replica’s IP address is stored.

		self.status = "NORMAL"			# The current status, either NORMAL, VIEW_CHANGE, or RECOVERY.
		self.v = 0						# The current view-number, initially 0.
		self.s = 0
		self.n = 0						# The op-number assigned to the most recently received request, initially 0.
		self.k = 0						# The commit-number is the op-number of the most recently committed operation.
		self.logs = list()				# The log. This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
		self.client_table = dict() 		# The client-table. This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
		self.config = list(replicas)	# The configuration. This is a sorted array containing the IP addresses of each of the 2f + 1 replicas.
		self.config.sort()
		self.replicas = replicas - {self}
		self.clients = clients
		self.quorumsize = quorumsize
		self.tout = tout

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

		# Control Variables
		self.done = False

		print("Replica:: Setup completed")

	async def run(self):
		while not self.done:
			if self.status == "NORMAL":
				if (self == self.config[self.v]):
					self.start_primary_timer()
				else:
					self.start_backup_timer()
			else:
				pass

		print("Replica:: Terminating now..")

	async def start_primary_timer(self):
		flag = True
		while flag:
			if self.num_req_latest > self.num_prepare_obsolete:
				self.num_req_obsolete = self.num_req_latest
				flag = False
	async def start_backup_timer(self):
		pass

	async def handle_client(self, reader, writer):
		while True:
			data = await reader.read(1024)
			if not data:
				break
			message = data.decode()
			print("Replica:: Received message:", message)
			writer.write(data)
			await writer.drain()

			(order, m) = pickle.loads(message)
			if order == "REQUEST":
				self.handle_request_message(m)

			if order == "PREPARE":
				self.handle_prepare_message(m)

			if order == 'PREPARE_OK':
				self.handle_prepare_ok_message(m)

		writer.close()

	def handle_request_message(self, message):
		if self == self.config[self.v] and self.status == "NORMAL":
			self.num_req_latest += 1
			(op, c, s) = message
			vs_max = self.get_vs_max(c) # It compares the request-number in the request with the information in the client table.


			if s > vs_max:
				self.n += 1 																	# The primary advances op-number
				self.logs.append((self.n, message))												# Appends the request to the end of the log
				self.client_table[c] = (s, False, None)											# Updates the information for this client in the client-table to contain the new request number 's'

				print("Replica:: Sending request to other replicas", self.s)
				for address in self.replicas:
					self.loop.run_until_complete(self.send(address[0], address[1], pickle.dumps(('PREPARE', (self.v, message, self.s))))) 	# Then it sends a (PREPARE v, m, n, k) message to the other replicas, n is the op-num it assigned to the request, k is the commit num

			# It will re-send the response, if the request is the most recent one from this client and it has already been executed.
			elif  s == vs_max and self.client_table[c][1] != "#":
					self.loop.run_until_complete(self.send(self.clients[c][0], self.clients[c][1], pickle.dumps(('REPLY',(self.v, message[2], self.client_table[message[1]][1])))))

			# If the request-number s isn’t bigger than the information in the table it drops the request
			else:
				pass

	def handle_prepare_message(self, message):
		if self != self.config[self.v] and self.status == "NORMAL":
			self.num_prepare_latest += 1
			(view_num, m, op_num, commit_num) = message

			print(f"Replica:: Prepare handler view_num={view_num} m={m} op_num={op_num} commit_num={commit_num}")
			if commit_num > self.last_commit_num:
				self.last_commit_num = commit_num

	def handle_prepare_ok_message(self, message):
		if self == self.config[self.v] and self.status == 'NORMAL':
			(view_num, op_num, i) = message
			print(f"Replica:: Prepare OK handler")
			pass



	def get_vs_max(self, c):
		if c not in self.client_table.keys():
			vs_max = -1
		else:
			vs_max =  self.client_table[c][0]			# max(setof(req, received(('REQUEST',_,_c, req))))
		return vs_max

class Client(AsyncNode):
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
		self.success = False
		print("Client::", self, "setup completed")


		# DS for measuring Performance
		self.perf_data = []
		self.cur_req = 0

	# ========================================================================
	# Client sends request one by one
	# Client Will always try to resend the request to primary if failed
	# Update request number only after the previous request has been completed
	# ========================================================================
	async def run(self):
		print("Client:: Running...")
		self.success = False

		# self.perf_data.append(time.perf_counter())
		payload = f"test#{random.randint(1, 100)}"

		# Sending request to primary
		while not self.success:
			self.loop.run_until_complete(self.send(self.primary, pickle.dumps(('REQUEST' , (payload, self.c, self.s)))))

			try:
				await asyncio.wait_for(self.wait_for_reply, self.tout) 				# Blocking thread until reply is received or timeout
				self.success = True
			except asyncio.TimeoutError:
				print("Client:: Timeout occured on REPLY for op", payload)

		print("Client:: Finished sending message to replica")

	async def wait_for_reply(self):
		flag = True
		while flag:
			if self.s in self.results:
				flag = False
				self.s += 1

	async def handle_client(self, reader, writer):
		while True:
			data = await reader.read(1024)
			if not data:
				break
			message = data.decode()
			print("Replica:: Received message:", message)
			writer.write(data)
			await writer.drain()

			(order, m) = pickle.loads(message)
			(view_number, c_id, result) = m
			if order == "REPLY":
				cur_req = cur_req + 1
				if self.v  != view_number:
					self.v = view_number
					self.primary = self.config[self.v]

				print("Client:: The current view is :", self.v)
				if c_id not in self.results:
					self.results[c_id] = result
				elif self.results[c_id] != result:
					print('Client:: Different result for request', c_id, result, 'than', self.results[c_id])
		writer.close()

	# def send_to_primary(self, payload):
	# 	# try:
	# 	print("Client:: Sending request to primary :", self.s, payload)
	# 	(order, reply) = pickle.loads(self.send_to(self.primary, pickle.dumps(('REQUEST' , (payload, self.c, self.s)))))
	# 	(view_number, c_id, result) = reply
	# 	print("Client:: Received message :", order, reply)
	# 		# if order == "REPLY":
	# 		# 	# tend = time.perf_counter()
	# 		# 	# self.perf_data[cur_req] = tend - self.perf_data[cur_req]
	# 		# 	cur_req = cur_req + 1
	# 		# 	if self.v  != view_number:
	# 		# 		self.v = view_number
	# 		# 		self.primary = self.config[self.v]

	# 		# 	print("Client:: The current view is :", self.v)
	# 		# 	if c_id not in self.results:
	# 		# 		self.results[c_id] = result
	# 		# 	elif self.results[c_id] != result:
	# 		# 		print('Client:: Different result for request', c_id, result, 'than', self.results[c_id])

	# 		# if self.s in self.results:
	# 		# 	self.s += 1
	# 		# 	return True

	# 	# except TimeoutError:
	# 	# 	print("Client:: Timeout occured on REPLY for op", self.curr_op)
	# 	# 	return False
	# ========================================================================