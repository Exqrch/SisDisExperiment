from multiprocessing import process
import sys
import os
import time
import random

MAX_NOPS = 10

def operation(i): return lambda state: (state+[i], ['result',i,'on',state])
operations = {i: operation(i) for i in range(MAX_NOPS)}

class Replica(process):
	def __init__(self, replicas:set, quorumsize:int, tout:float):

		# State vsriables for the replica
		# We don't need The replica number. This is the index into the con- figuration where this replica’s IP address is stored.

		self.status = "normal"			# The current status, either normal, view-change, or recovering.
		self.v = 0						# The current view-number, initially 0.
		self.s = 0
		self.n = 0						# The op-number assigned to the most recently received request, initially 0.
		self.k = 0						# The commit-number is the op-number of the most recently committed operation.
		self.logs = list()				# The log. This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
		self.client_table = dict() 		# The client-table. This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
		self.config = list(replicas)	# Theconfiguration. This is a sorted array containing the IP addresses of each of the 2f + 1 replicas.
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

class Client(process):
	def __init__(self, replicas:Replica, n_requests:int, tout:float):
		self.state = None												# Current State of the client
		self.config = list(replicas)
		self.config.sort()												# Configuration setting of the current replica group
		self.v = 0														# View Number for the current request
		self.c = self													# Client Id
		self.s  = 0														# Request Number
		self.primary = self.config[self.v]
		self.results = dict()
		print("Client", self, "setup completed")


		# DS for measuring Performance
		self.perf_data = []
		self.cur_req = 0