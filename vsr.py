from multiprocessing import process
from network import AsyncNode
import sys
import os
import time
import random
import pickle
import asyncio

MAX_NOPS = 10


def operation(i): return lambda state: (state+[i], ['result', i, 'on', state])


operations = {i: operation(i) for i in range(MAX_NOPS)}


class Replica(AsyncNode):
    def __init__(self, replicas: list, quorumsize: int, tout: float, clients: dict, ip, port):
        super().__init__(ip, port)
        # State variables for the replica
        # We don't need The replica number. This is the index into the configuration where this replica’s IP address is stored.

        # The current status, either NORMAL, VIEW_CHANGE, or RECOVERY.
        self.status = "NORMAL"
        self.v = 0						# The current view-number, initially 0.
        self.s = 0
        # The op-number assigned to the most recently received request, initially 0.
        self.n = 0
        # The commit-number is the op-number of the most recently committed operation.
        self.k = 0
        # The log. This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
        self.logs = list()
        # The client-table. This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
        self.client_table = dict()
        # The configuration. This is a sorted array containing the IP addresses of each of the 2f + 1 replicas.
        self.config = replicas.copy()
        self.config.sort()
        self.replicas = self.config.copy()
        self.replicas.remove(port)
        self.clients = clients
        self.quorumsize = quorumsize
        self.tout = tout

        # Timeout and heartbeat Implementation variables
        self.num_req_latest = 0
        self.num_req_obsolete = 0
        self.num_prepare_latest = 0
        self.num_prepare_obsolete = 0
        self.heartbeat = 0
        self.last_commit_num = 0
        self.num_prepare_ok = 0
        self.num_do_view_change = 0
        self.last_received_prepare_op_num = 0

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
                    try:
                        # Blocking thread until reply is received or timeout
                        await asyncio.wait_for(self.start_primary_timer(), self.tout)
                        self.success = True
                    except asyncio.TimeoutError:
                        print("Replica:: Primary timeout occured, sending COMMIT")
                        # TODO
                        pass
                else:
                    try:
                        # Blocking thread until reply is received or timeout
                        await asyncio.wait_for(self.start_backup_timer(), self.tout + 2)
                        self.success = True
                    except asyncio.TimeoutError:
                        print(
                            "Replica:: Backup timeout occured, requesting VIEW_CHANGE")
                        self.last_normal_view = self.v
                        self.start_view_change_req.clear()
                        self.v = (self.v + 1) % len(self.replicas)
                        self.status = 'VIEW_CHANGE'
                        for replica in self.replicas:
                            # Then it sends a (PREPARE v, m, n, k) message to the other replicas, n is the op-num it assigned to the request, k is the commit num
                            self.new_loop().run_until_complete(
                                self.send(replica, pickle.dumps(('START_VIEW_CHANGE', (self.v, self.s)))))
                        pass
            else:
                pass

        print("Replica:: Terminating now..")

    def commit_prev_operation(self):
        """
        Section 4, page 4, Point 7
        When a backup learns of a commit, it waits un- til it has the request in its log (which may require state transfer)
        and until it has executed all earlier operations. Then it executes the operation by per- forming the up-call to the
        service code, increments its commit-number, updates the client’s entry in the client-table, but does not send the
        reply to the client.
        """
        has_last_commited = False
        for (op, m) in self.logs:
            if op == self.last_commit_num:
                has_last_commited = True

        if (self.k < self.last_commit_num and has_last_commited):
            my_commit = self.k + 1
            print("Replica:: Backup: Some pending operations will be commited now...")
            for op in range(my_commit, has_last_commited):
                message = self.update_and_get_logs(op)
                (oper, client, req) = message[0]
                self.k += 1
                self.client_table[client][1] = True
                self.client_table[client][2] = "answer = " + str(req)
                print(
                    f"Replica:: Backup: Completing old operations.. Commit number {self.k} and operation {oper}")

            self.k += 1
            self.client_table[m[1]][1] = True
            self.client_table[m[1]][2] = "answer = " + str(m[2])
            print(
                f"Replica:: Backup: Latest commit number {self.k} and operation {m[0]}")

    def update_and_get_logs(self, curr_op):
        message = []
        for (oper, m) in self.logs:
            message.append([m, (oper, m), oper == curr_op])
        return message

    async def start_primary_timer(self):
        print("Replica:: Primay: Starting primary timer")
        condition = asyncio.Condition()
        async with condition:
            await condition.wait_for(lambda: self.num_req_latest > self.num_prepare_obsolete)
            self.num_req_obsolete = self.num_req_latest

    async def start_backup_timer(self):
        """
        Section 4, normal operation, point 4, page 7
        Backups process PREPARE messages in order: a backup won't accept a prepare with op-number n until
        it has entries for all earlier requests in its log. When a backup i receives a PREPARE message, it
        waits until it has entries in its log for all earlier requests (doing state transfer if necessary
        to get the missing information).
        """
        flag = True
        print("Replica:: Backup: Waiting for prepare having next op num:", self.n+1)
        while flag:
            if self.last_received_prepare_op_num == self.n + 1:  					# Await untill it has all previous entries
                (view_num, m, op_num, commit_num) = self.last_received_message
                self.commit_prev_operation()
                self.n += 1															# Then increments it's op_num
                self.logs.append((self.n, m))										# Append the logs
                # Update the client_table
                self.client_table[m[1]] = [m[2], False, None]
                print("Replica:: Backup: Sending PREPARE_OK to primary...")
                self.new_loop().run_until_complete(self.send(
                    self.config[self.v], pickle.dumps(('PREPARE_OK', (self.v, self.n, self.port)))))
                flag = False

            elif self.num_prepare_latest > self.num_prepare_obsolete or self.heartbeat == 1:
                self.num_prepare_obsolete = self.num_prepare_latest
                print(
                    "Replica:: Backup: Prepare message / heartbeat from the primary received...")
                self.commit_prev_operation()
                self.heartbeat = 0

    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()

            (order, m) = pickle.loads(data)
            print(f"Replica:{self.port}: Received message:", order, m)
            if order == "REQUEST":
                self.handle_request_message(m)

            if order == "PREPARE":
                self.handle_prepare_message(m)

            if order == 'PREPARE_OK':
                self.handle_prepare_ok_message(m)

            if order == "COMMIT":
                self.handle_commit_message(m)

            if order == "START_VIEW":
                self.handle_start_view_message(m)

            if order == "DO_VIEW_CHANGE":
                self.handle_do_view_change_message(m)

            if order == "START_VIEW_CHANGE":
                self.handle_start_view_change_message(m)

        writer.close()

    def handle_commit_message(self, message):
        if self.port != self.config[self.v] and self.status == "NORMAL":
            (v, k) = message
            print("Replica:: Backup: Received Hearbear/COMMIT from the primary...")
            self.heartbeat = 1
            if k > self.last_commit_num:
                self.last_commit_num = k

    def handle_start_view_message(self, message):
        '''
        Section 4, view change mode, point 5 page 6
        When other replicas receive the STARTVIEW message, they replace their log
        with the one in the message, set their op-number to that of the latest entry
        in the log, set their view-number to the view num- ber in the message, change
        their status to normal, and update the information in their client-table.
        Then they execute all op- erations known to be committed that they haven't
        executed previously, advance their commit-number, and update the information
        in their client-table.
        '''
        (view_num, new_logs, op_num, commit_num, rep) = message

        if self.config[view_num] != rep:
            print("Replica:: ERROR: Primary mismatch")

        if self.status == "VIEW_CHANGE":
            self.status = "NORMAL"
            self.v = view_num
            self.n = op_num
            self.logs = new_logs
            print("Replica:: Move to new view", view_num)

            if commit_num < self.n:
                print("Replica:: Sending PREPARE_OK to primary", self.v, self.n)
                self.new_loop().run_until_complete(self.send(
                    self.config[self.v], pickle.dumps(('PREPARE_OK', (self.v, self.n, self.port)))))
                start = self.k + 1
                for operation in range(start, commit_num+1):
                    client_info = self.update_and_get_logs(operation)
                    (oper, client, req) = client_info[0]

                    # Perform client operation at this place.
                    # updates the client’s entry in the client-table,
                    self.client_table[client][1] = True
                    self.client_table[client][2] = "answer = " + str(req)
                    self.k += 1
                    print(
                        "#### START_VIEW: Completing old operations..current commit number and op", k, cur_msg[0])

    def handle_do_view_change_message(self, message):
        '''
        (From vr-revis, page 5, 4.2 View Changes, point 3)
        When the new primary receives f + 1 DOVIEWCHANGE messages from different replicas
        including itself, it sets its view-number to that in the messages and selects
        as the new log the one contained in the message with the largest v′; if several
        messages have the same v' it selects the one among them with the largest n. It
        sets its op-number to that of the topmost entry in the new log, sets its commit-number
        to the largest such number it received in the DOVIEWCHANGE mes- sages, changes its
        status to normal, and informs the other replicas of the completion of the view
        change by sending ⟨STARTVIEW v, l, n, k⟩ messages to the other replicas, where
        l is the new log, n is the op-number, and k is the commit-number.

        (From vr-revis, page 6, 4.2 View Changes, point 4)
        The new primary starts accepting client requests. It also executes (in order) any
        committed operations that it hadn't executed previously, updates its client table,
        and sends the replies to the clients.
        '''
        if self.status == 'VIEW_CHANGE':
            print("Replica:: DO_VIEW_CHANGE handler")

        pass

    def handle_start_view_change_message(self, message):
        pass

    def handle_request_message(self, message):
        print(self.port, self.config, self.status)
        if self.port == self.config[self.v] and self.status == "NORMAL":
            print("Replica:: REQUEST handler", message)
            self.num_req_latest += 1
            (op, c, s) = message
            # It compares the request-number in the request with the information in the client table.
            vs_max = self.get_vs_max(c)

            if s > vs_max:
                self.n += 1 																	# The primary advances op-number
                # Appends the request to the end of the log
                self.logs.append((self.n, message))
                # Updates the information for this client in the client-table to contain the new request number 's'
                self.client_table[c] = (s, False, None)

                print("Replica:: Sending request to other replicas", self.s)
                self.num_prepare_ok = 0
                for address in self.replicas:
                    # Then it sends a (PREPARE v, m, n, k) message to the other replicas, n is the op-num it assigned to the request, k is the commit num
                    self.new_loop().run_until_complete(
                        self.send(address, pickle.dumps(('PREPARE', (self.v, message, self.n, self.k)))))

            # It will re-send the response, if the request is the most recent one from this client and it has already been executed.
            elif s == vs_max and self.client_table[c][1]:
                self.new_loop().run_until_complete(self.send(self.clients[c], pickle.dumps(
                    ('REPLY', (self.v, message[2], self.client_table[message[1]][1])))))

            # If the request-number s isn’t bigger than the information in the table it drops the request
            else:
                pass

    def handle_prepare_message(self, message):
        """
        Section 4, normal operation, point 4, page 7
        Backups process PREPARE messages in order: a backup won't accept a prepare with op-number n until
        it has entries for all earlier requests in its log. When a backup i receives a PREPARE message, it
        waits until it has entries in its log for all earlier requests (doing state transfer if necessary
        to get the missing information).
        Handling this inside the run method as its a bad idea to wait inside the event handler.
        """
        if self.port != self.config[self.v] and self.status == "NORMAL":
            self.num_prepare_latest += 1
            (view_num, m, op_num, commit_num) = message
            self.last_received_prepare_op_num = op_num
            self.last_received_message = message

            print(
                f"Replica:: PREPARE handler view_num={view_num} m={m} op_num={op_num} commit_num={commit_num}")
            if commit_num > self.last_commit_num:
                self.last_commit_num = commit_num

    def handle_prepare_ok_message(self, message):
        if self.port == self.config[self.v] and self.status == 'NORMAL':
            (view_num, op_num, i) = message
            self.num_prepare_ok += 1

            print(f"Replica:: PREPARE_OK handler num_prepare_ok",
                  self.num_prepare_ok)
            if self.num_prepare_ok >= self.quorumsize:
                start = self.k + 1
                for operation in range(start, op_num + 1):
                    print(
                        "Replica:: Primay has some uncommited operations. Committing now...")
                    client_info = self.update_and_get_logs(operation)
                    (oper, client, req) = client_info[0]
                    if (req > self.client_table[client][0] or self.client_table[client][1] == False):
                        # Do client operation here
                        self.client_table[client] = (
                            req, True, "answer = " + str(client) + " " + str(oper))
                        self.k += 1
                        print(
                            f"Replica:: Primary: Commit number {self.k} and operation {oper}")
                        self.new_loop().run_until_complete(self.send(client, pickle.dumps(
                            ('REPLY', (self.v, req, self.client_table[client][2])))))

    def get_vs_max(self, c):
        if c not in self.client_table.keys():
            vs_max = -1
        else:
            # max(setof(req, received(('REQUEST',_,_c, req))))
            vs_max = self.client_table[c][0]
        return vs_max


class Client(AsyncNode):
    def __init__(self, replicas: list, tout, ip, port):
        super().__init__(ip, port)

        # Time keeping
        self.start_time = time.monotonic()
        self.query_time = []
        self.end_time = None

        self.state = None												# Current State of the client
        self.config = replicas
        self.config.sort()												# Configuration setting of the current replica group
        self.v = 0														# View Number for the current request
        self.c = self.port													# Client Id
        self.s = 0														# Request Number
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
            loop = self.new_loop()
            loop.run_until_complete(self.send(self.primary, pickle.dumps(
                ('REQUEST', (payload, self.c, self.s)))))

            try:
                # Blocking thread until reply is received or timeout
                await asyncio.wait_for(self.wait_for_reply(), self.tout)
                self.success = True
            except asyncio.TimeoutError:
                print("Client:: Timeout occured on REPLY for op", payload)

        print("Client:: Finished sending message to replica")

    async def wait_for_reply(self):
        condition = asyncio.Condition()
        async with condition:
            await condition.wait_for(lambda: self.s in self.results)
            self.s += 1

    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            message = data.decode()
            print("Client:: Received message:", message)
            writer.write(data)
            await writer.drain()

            (order, m) = pickle.loads(message)
            (view_number, c_id, result) = m
            if order == "REPLY":
                self.cur_req += 1
                if self.v != view_number:
                    self.v = view_number
                    self.primary = self.config[self.v]

                print("Client:: The current view is :", self.v)
                if c_id not in self.results:
                    self.results[c_id] = result
                elif self.results[c_id] != result:
                    print('Client:: Different result for request',
                          c_id, result, 'than', self.results[c_id])
        writer.close()
    # ========================================================================
