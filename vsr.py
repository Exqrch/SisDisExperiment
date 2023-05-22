from network import Node
import time
import random
import json
import socket
import logging
import threading

class Replica(Node):
    def __init__(
        self,
        network,
        node_id,
        node_address,
        quorumsize: int,
        tout: float,
        clients: dict,
    ):
        super().__init__(network, node_id, node_address)
        # State variables for the replica

        self.status = "NORMAL"
        self.node_id = node_id
        self.view_num = 0
        self.req_num = 0
        # The most recently received op-num from request
        self.op_num = 0
        # The commit-number is the op-number of the most recently committed operation.
        self.commit_num = 0
        # The log. This is an array containing op-number entries. The entries contain the requests that have been received so far in their assigned order.
        self.logs = list()
        # The client-table. This records for each client the number of its most recent request, plus, if the request has been executed, the result sent for that request.
        self.client_table = dict()
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
        self.last_prepare_message = None

        # View Change implementation variable
        self.start_view_change_req = list()
        self.do_view_change_req = list()
        self.last_normal_view = 0

        # Control Variables
        self.done = False

        print(f"Replica:{self.node_id}: Setup completed")

    def send_to_client(self, message, c_id=5000):
        receiver_address = self.clients[c_id]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(receiver_address)
            s.sendall(message.encode("utf-8"))

    def run(self):
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.name = f"listen_thread_{self.node_id}"
        listen_thread.start()

        run_thread = threading.Thread(target=self.start)
        run_thread.name = f"run_thread_{self.node_id}"
        run_thread.start()

    def start(self):
        while not self.done:
            if self.status == "NORMAL":
                if self.is_primary_replica():
                    self.start_primary_timer()

                    # Timeout
                    print(f"Replica:{self.node_id}: Primary timeout occured, sending COMMIT")
                    message = ("COMMIT", (self.view_num, self.commit_num))
                    self.broadcast(json.dumps(message))

                else:
                    self.start_backup_timer()

                    # Timeout
                    print(f"Replica:{self.node_id}: Backup timeout occured, requesting VIEW_CHANGE")
                    self.last_normal_view = self.view_num
                    self.start_view_change_req.clear()
                    self.view_num = (self.view_num + 1) % len(self.network.nodes)
                    self.status = "VIEW_CHANGE"

                    message = (
                        "START_VIEW_CHANGE",
                        (self.view_num, self.req_num),
                    )
                    self.broadcast(json.dumps(message))
            else:
                pass

        print(f"Replica:{self.node_id}: Terminating now..")

    def commit_prev_operation(self):
        """
        Section 4, page 4, Point 7
        When a backup learns of a commit, it waits un- til it has the request in its log (which may require state transfer)
        and until it has executed all earlier operations. Then it executes the operation by per- forming the up-call to the
        service code, increments its commit-number, updates the client's entry in the client-table, but does not send the
        reply to the client.
        """
        has_last_commited = False
        for op, m in self.logs:
            if op == self.last_commit_num:
                has_last_commited = True

        if self.commit_num < self.last_commit_num and has_last_commited:
            my_commit = self.commit_num + 1
            print(f"Replica:{self.node_id}: Backup: Some pending operations will be commited now...")
            for op in range(my_commit, has_last_commited):
                message = self.get_matching_logs(op)
                (payload, client, req) = message
                self.commit_num += 1
                self.client_table[client][1] = True
                self.client_table[client][2] = payload
                print(
                    f"Replica:{self.node_id}: Backup: Completing old operations.. Commit number {self.commit_num} and payload {payload}"
                )

            self.commit_num += 1
            self.client_table[m[1]][1] = True
            self.client_table[m[1]][2] = m[2]
            print(
                f"Replica:{self.node_id}: Backup: Latest commit number {self.commit_num} and operation {m[0]}"
            )

    def get_matching_logs(self, curr_op):
        for oper, m in self.logs:
            if oper == curr_op:
                return m

    def start_primary_timer(self):
        timer_end = time.monotonic_ns() + self.tout * 10**9
        print(f"Replica:{self.node_id}: Primary timer starts", time.monotonic_ns(), timer_end)
        while time.monotonic_ns() < timer_end:
            if self.num_req_latest > self.num_prepare_obsolete:
                self.num_req_obsolete = self.num_req_latest
                timer_end += self.tout * 10**9

    def start_backup_timer(self):
        """
        Section 4, normal operation, point 4, page 7
        Backups process PREPARE messages in order: a backup won't accept a prepare with op-number n until
        it has entries for all earlier requests in its log. When a backup i receives a PREPARE message, it
        waits until it has entries in its log for all earlier requests (doing state transfer if necessary
        to get the missing information).
        """
        print(
            f"Replica:{self.node_id}: Backup: Waiting for prepare having next op num:",
            self.op_num + 1,
        )
        timer_end = time.monotonic_ns() + self.tout * 10**9
        print(f"Replica:{self.node_id}: Backup timer starts", time.monotonic_ns(), timer_end)
        while time.monotonic_ns() < timer_end:
            if (
                self.num_prepare_latest == self.op_num + 1
            ):  # Await untill it has all previous entries
                print(self.last_message)
                (view_num, m, op_num, commit_num) = self.last_prepare_message
                self.commit_prev_operation()
                self.op_num += 1  # Then increments it's op_num
                self.logs.append((self.op_num, m))  # Append the logs
                # Update the client_table
                self.client_table[m[1]] = [m[2], False, None]

                print(f"Replica:{self.node_id}: Backup: Sending PREPARE_OK to primary...")
                message = ("PREPARE_OK", (self.view_num, self.op_num, self.node_id))
                self.send_to(self.curr_primary_id(), json.dumps(message))
                timer_end += self.tout * 10**9

            elif (
                self.num_prepare_latest > self.num_prepare_obsolete
                or self.heartbeat == 1
            ):
                self.num_prepare_obsolete = self.num_prepare_latest
                print(
                    f"Replica:{self.node_id}: Backup: Prepare message / heartbeat from the primary received..."
                )
                self.commit_prev_operation()
                self.heartbeat = 0
                timer_end += self.tout * 10**9

    def listen(self):
        self.socket.listen()
        while True:
            try:
                conn, _ = self.socket.accept()
                data = conn.recv(1024)
                message = data.decode("utf-8")
                self.last_message = json.loads(message)
            except OSError:
                break

            (order, m) = self.last_message
            print(f"Replica:{self.node_id}: Received message:", order, m)

            if order == "REQUEST":
                self.handle_request_message(m)

            if order == "READ":
                self.handle_read_message(m)

            if order == "PREPARE":
                self.handle_prepare_message(m)

            if order == "PREPARE_OK":
                self.handle_prepare_ok_message(m)

            if order == "COMMIT":
                self.handle_commit_message(m)

            if order == "START_VIEW":
                self.handle_start_view_message(m)

            if order == "DO_VIEW_CHANGE":
                self.handle_do_view_change_message(m)

            if order == "START_VIEW_CHANGE":
                self.handle_start_view_change_message(m)
            conn.close()

    def handle_read_message(self, message):
        if self.is_primary_replica():
            print(f"Replica:{self.node_id}: Primary: READ message handler")
            self.num_req_latest += 1
            (k) = message

            print(f"Replica:{self.node_id}: Primary: REQUEST handler", message)
            (op, c_id, s) = message

            payload =  ("REPLY", (self.view_num, s, None))
            for i in range(len(self.logs) - 1, -1, -1):
                value = self.logs[i]
                if value[0] == k:
                    payload =  ("REPLY", (self.view_num, s, value))
                    self.send_to_client(json.dumps(payload))
                    break
            self.send_to_client(json.dumps(payload))

            # It compares the request-number in the request with the information in the client table.
            vs_max = self.get_vs_max(c_id)

            if s > vs_max:
                # Updates the information for this client in the client-table to contain the new request number 's'
                self.client_table[c_id] = (s, True, payload[1][2])

    def handle_commit_message(self, message):
        if self.is_backup_replica():
            (v, k) = message
            print(f"Replica:{self.node_id}: Backup: Received Hearbear/COMMIT from the primary...")
            self.heartbeat = 1
            if k > self.last_commit_num:
                self.last_commit_num = k

    def handle_start_view_message(self, message):
        """
        Section 4, view change mode, point 5 page 6
        When other replicas receive the STARTVIEW message, they replace their log
        with the one in the message, set their op-number to that of the latest entry
        in the log, set their view-number to the view num- ber in the message, change
        their status to normal, and update the information in their client-table.
        Then they execute all op- erations known to be committed that they haven't
        executed previously, advance their commit-number, and update the information
        in their client-table.
        """
        (view_num, new_logs, op_num, commit_num, rep) = message

        if self.status == "VIEW_CHANGE":
            print(f"Replica:{self.node_id}: Backup: Starting a new view {view_num}")
            self.status = "NORMAL"
            self.view_num = view_num
            self.op_num = op_num
            self.logs = new_logs
            print(f"Replica:{self.node_id}: Move to new view", view_num)

            if commit_num < self.op_num:
                print(
                    f"Replica:{self.node_id}: Sending PREPARE_OK to primary",
                    self.view_num,
                    self.op_num,
                )
                message = ("PREPARE_OK", (self.view_num, self.op_num, self.node_id))
                self.send_to(self.curr_primary_id(), json.dumps(message))

                start = self.commit_num + 1
                for operation in range(start, commit_num + 1):
                    client_info = self.get_matching_logs(operation)
                    (payload, client, req) = client_info

                    # Perform client operation at this place.
                    # updates the client’s entry in the client-table,
                    self.client_table[client][1] = True
                    self.client_table[client][2] = payload
                    self.commit_num += 1
                    print(
                        "#### START_VIEW: Completing old operations..current commit number and op",
                        self.commit_num,
                        self.op_num,
                    )

    def handle_do_view_change_message(self, message):
        """
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
        """
        if self.status == "VIEW_CHANGE":
            print(f"Replica:{self.node_id}: DO_VIEW_CHANGE handler")
            (view_num, logs, last_view, op_num, commit_num, node_id) = message
            self.do_view_change_req.append(message)

            # Check how many do view change
            if len(self.do_view_change_req) >= self.quorumsize:   
                self.view_num = view_num
                # it sets its view-number to that in the messages
                max_last_view_num = max(self.last_normal_view, max(self.do_view_change_req, key=lambda req: req[2])[2])
                
                # It sets its op-number to that of the topmost entry in the new log and selects the new log the one contained in the message with the largest v'
                new_op_num = self.op_num
                for req in self.do_view_change_req:
                    if req[2] == max_last_view_num and req[3] > new_op_num and len(req[1]) > 0:
                        new_op_num = req[3]
                        self.logs = req[1]
                
                if len(self.logs) > 0:
                    self.op_num = self.logs[-1][0]
                    print(f"Replica:{self.node_id}: Primary: Setting op number to {self.op_num}")
                
                latest_commit_num = max(self.last_normal_view, max(self.do_view_change_req, key=lambda req: req[3])[3])
                self.commit_num = latest_commit_num
                self.status = "NORMAL"
                
                print(f"Replica:{self.node_id}: Primary: Starting a new view {self.view_num}")
                self.do_view_change_req.clear()
                message = ("START_VIEW", (self.view_num, self.logs, self.op_num, self.commit_num, self.node_id))
                self.broadcast(json.dumps(message))
                message = ("VIEW_NUMBER", (self.view_num, self.node_id))
                self.send_to_client(json.dumps(message))                

    def handle_start_view_change_message(self, message):
        '''
		(From vr-revis, page 5, 4.2 View Changes, point 1
		A replica notices the need for a view change either based on its own timer, or because
		it receives a STARTVIEWCHANGE or DOVIEWCHANGE message for a view with a larger number 
		than its own view-number.
		'''
        print(f"Replica:{self.node_id}: START_VIEW_CHANGE handler")
        (view_num, req_num) = message
        if view_num > self.view_num and self.status == "NORMAL":
            self.last_normal_view = self.view_num
            self.view_num = view_num
            self.start_view_change_req.clear()
            self.status = "VIEW_CHANGE"
            message = (
                "START_VIEW_CHANGE",
                (self.view_num, self.req_num),
            )
            self.broadcast(json.dumps(message))

        '''
		(From vr-revis, page 5, 4.2 View Changes, point 2)[LOC 104-117]
		When replica i receives STARTVIEWCHANGE messages for its view-number from f other 
		replicas, it sends a ⟨DOVIEWCHANGE v, l, v', n, k, i⟩ message to the node that 
		will be the primary in the new view. Here v is its view-number, l is its log, 
		v' is the view number of the latest view in which its status was normal, n is 
		the op-number, and k is the commit- number.
		'''
        if view_num == self.view_num and self.status == "VIEW_CHANGE":
            self.start_view_change_req.append(message)
            print(f"Replica:{self.node_id}: view_change_req {self.start_view_change_req}")
            if len(self.start_view_change_req) >= self.quorumsize:
                print(f"Replica:{self.node_id}: Backup: Sending DO_VIEW_CHANGE to new primary {self.curr_primary_id()}")
                message = ('DO_VIEW_CHANGE', (self.view_num, self.logs, self.last_normal_view, self.op_num, self.commit_num, self.node_id))
                self.send_to(self.curr_primary_id(), json.dumps(message))

    def handle_request_message(self, message):
        if self.is_primary_replica():
            print(f"Replica:{self.node_id}: Primary: REQUEST handler", message)
            self.num_req_latest += 1
            (op, c_id, s) = message
            # It compares the request-number in the request with the information in the client table.
            vs_max = self.get_vs_max(c_id)

            if s > vs_max:
                self.op_num += 1  # The primary advances op-number
                # Appends the request to the end of the log
                self.logs.append((self.op_num, message))
                # Updates the information for this client in the client-table to contain the new request number 's'
                self.client_table[c_id] = (s, False, None)

                print(f"Replica:{self.node_id}: Sending request to other replicas", self.req_num)
                self.num_prepare_ok = 0
                message = (
                    "PREPARE",
                    (
                        self.view_num,
                        message,
                        self.op_num,
                        self.commit_num,
                    ),
                )
                self.broadcast(json.dumps(message))

            # It will re-send the response, if the request is the most recent one from this client and it has already been executed.
            elif s == vs_max and self.client_table[c_id][1]:
                message = (
                    "REPLY",
                    (
                        self.view_num,
                        message[2],
                        self.client_table[c_id][2],
                    ),
                )
                self.send_to_client(json.dumps(message), c_id)

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
        if self.is_backup_replica():
            self.num_prepare_latest += 1
            (view_num, m, op_num, commit_num) = message
            self.last_prepare_message = message

            print(
                f"Replica:{self.node_id}: Backup: PREPARE handler view_num={view_num} m={m} op_num={op_num} commit_num={commit_num}"
            )
            if commit_num > self.last_commit_num:
                self.last_commit_num = commit_num

    def handle_prepare_ok_message(self, message):
        if self.is_primary_replica():
            (view_num, op_num, i) = message
            self.num_prepare_ok += 1

            print(f"Replica:{self.node_id}: Primary: PREPARE_OK handler num_prepare_ok", self.num_prepare_ok)
            if self.num_prepare_ok >= self.quorumsize:
                start = self.commit_num + 1
                for operation in range(start, op_num + 1):
                    print(
                        f"Replica:{self.node_id}: Primay has some uncommited operations. Committing now..."
                    )
                    client_info = self.get_matching_logs(operation)
                    print("==========LOGS==============", self.logs)
                    print("==========CLIENT INFO=======", client_info)
                    print("==========CLIENT TABLE======", self.client_table)
                    (payload, client, req) = client_info
                    if (
                        req > self.client_table[client][0]
                        or self.client_table[client][1] == False
                    ):
                        # Do client operation here
                        self.client_table[client] = (req, True, payload)
                        self.commit_num += 1
                        print(
                            f"Replica:{self.node_id}: Primary: Commit number {self.commit_num} and payload {payload}"
                        )
                        message = (
                            "REPLY",
                            (
                                self.view_num,
                                req,
                                self.client_table[client][2],
                            ),
                        )
                        print("REPLY message", message)
                        self.send_to_client(json.dumps(message), client)

    def get_vs_max(self, c_id):
        if c_id not in self.client_table.keys():
            vs_max = -1
        else:
            vs_max = self.client_table[c_id][0]
        return vs_max

    def curr_primary_id(self):
        return self.view_num % len(self.network.nodes)

    def is_backup_replica(self):
        return (
            self.node_id != self.view_num % len(self.network.nodes)
            and self.status == "NORMAL"
        )

    def is_primary_replica(self):
        return (
            self.node_id == self.view_num % len(self.network.nodes)
            and self.status == "NORMAL"
        )


class Client:
    def __init__(self, replicas: list, tout, ip, port):
        # Socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((ip, port))

        # Time keeping
        self.start_time = time.monotonic_ns()
        self.query_time = []
        self.end_time = None

        self.state = None
        # Configuration setting of the current replica group
        self.config = replicas.copy()
        self.view_num = 0
        self.c_id = port
        self.req_num = 1
        self.primary = self.config[self.view_num]
        self.results = dict()
        self.tout = tout
        self.success = False
        print("Client::", self, "setup completed")

    # ========================================================================
    # Client sends request one by one
    # Client Will always try to resend the request to primary if failed
    # Update request number only after the previous request has been completed
    # ========================================================================
    def run(self):
        print("Client:: Running...")
        timer = time.monotonic_ns()
        self.success = False

        with open('queries/best/vsr/query_1.txt', 'r') as f:
            for i, message in enumerate(f.readlines()):
                query_timer = time.monotonic_ns()
                [query, payload] = message.strip().split("-")
                # Sending request to primary
                if message == "":
                    break
                if query == "SET":
                    while not self.success:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            message = json.dumps(
								("REQUEST", (payload, self.c_id, self.req_num))
							)
                            s.connect(self.primary)
                            s.sendall(message.encode("utf-8"))

                        self.start_timer()
                else:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        data = json.dumps(("READ", (payload, self.c_id, self.req_num)))
                        s.connect(self.primary)
                        s.sendall(data.encode("utf-8"))

                        self.start_timer()
                query_timer_end = time.monotonic_ns()
                print(f"Query {query}-{payload} took: {query_timer_end - query_timer} start time {query_timer} end time {query_timer_end}")
                print(f"LAST VIEW NUM {self.view_num}")
        

        

        print("Client:: Finished sending message to replica")
        timer_end = time.monotonic_ns()
        print(f"Runtime Taken {timer_end - timer} start time {timer} end time {timer_end}")

    def start_timer(self):
        timer_end = time.monotonic_ns() + self.tout * 10**9
        print("Client:: Timer start and end", time.monotonic_ns(), timer_end)
        while time.monotonic_ns() < timer_end:
            print("timer", self.req_num, list(self.results.keys()))
            if self.req_num in list(self.results.keys()):
                self.req_num += 1
                self.success = True
                break
        if self.success:
            print("Client:: Timeout....")

    def handle_reply_message(self, message):
        (view_number, req_num, result) = message
        if self.view_num != view_number:
            self.view_num = view_number
            self.primary = self.config[self.view_num]

        if req_num not in self.results:
            self.results[req_num] = result
        elif self.results[req_num] != result:
            print(
                "Client:: Different result for request",
                req_num,
                result,
                "than",
                self.results[req_num],
            )
        print(
            "Client:: The current view is :",
            self.view_num,
            "message req_num",
            req_num,
            "results",
            self.results,
        )

    def handle_view_number_message(self, message):
        (view_number, node_id) = message
        if self.view_num != view_number:
            self.view_num = view_number
            self.primary = self.config[self.view_num]

    def listen(self):
        self.socket.listen()
        while True:
            try:
                conn, _ = self.socket.accept()
                data = conn.recv(1024)
                message = data.decode("utf-8")
                self.last_message = json.loads(message)
                conn.close()
            except OSError:
                break

            (order, m) = self.last_message
            print("Client:: RECEIVED", order, m)
            if order == "REPLY":
                self.handle_reply_message(m)
            if order == "VIEW_NUMBER":
                self.handle_view_number_message(m)

    # ========================================================================
