import logging
import threading
import time
from node_socket import UdpSocket
import json
import random

def thread_exception_handler(args):
    logging.error(f"Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))


class Quorum:

    def __init__(self, node_id: int, port: int, neighbors_ports: list, lb_fault_duration: int, is_continue: bool,
                 heartbeat_duration: float, leader_id_callback):
        
        # Initiate Needed Variable
        self.port = port
        self.neighbors_ports = neighbors_ports
        self.new_neighbors_ports = neighbors_ports
        self.node_id = node_id
        
        self.heartbeat_duration = heartbeat_duration

        self.term = 0
        self.vote = {}
        self.election_timer = round(random.uniform(lb_fault_duration, lb_fault_duration+4),1)
        self.alive_ports = neighbors_ports
        self.voter = []
        self.iteration = 2

        self.role = 'follower'
        self.leader_ports = 0
        self.logs = {}

        self.is_continue = is_continue
        self.lb_fault_duration = lb_fault_duration

        self.stop_signal = threading.Event()
        self.leader_id_callback = leader_id_callback

        self.thread = None


    def reset_timer(self):
        # Function to reset timer with new thread every reset
        self.election_timer = round(random.uniform(self.lb_fault_duration, self.lb_fault_duration+4),1)

    def election_timer_procedure(self):
        # Initiate timer for election

        while self.timer_thread_flag:
            # Run Election Timer per 0.1s
            time.sleep(0.1)
            self.election_timer = round(self.election_timer - 0.1, 1)
            
            if self.role == "follower":
                if self.election_timer == 0.0:
                    # If not hearing from leader for certain time, be candidate and vote for itself
                    self.role = 'candidate'
                    self.term += 1
                    if self.leader_ports != 0:
                        self.alive_ports.remove(self.leader_ports)
                    reset_thread = threading.Thread(target=self.reset_timer)
                    self.iteration += 1
                    reset_thread.name=f"Thread-{self.iteration}"
                    reset_thread.start()
                    self.vote[self.term] = self.node_id
                    self.voter.append(self.node_id)
                    # Send message to other nodes to request for vote
                    for neighbor in self.neighbors_ports:
                        message = {}
                        message['sender'] = self.node_id
                        message['term'] = self.term
                        message['message'] = 'vote_request'
                        message['ports'] = self.port
                        message['sender_port'] = self.port
                        UdpSocket.send(json.dumps(message),neighbor)
            if self.role == "candidate":
                if len(self.voter) > len(self.alive_ports)/2:
                    # If got votes more than half of alive nodes, be leader
                    self.role = 'leader'
                    self.leader_id_callback(self.node_id)
                    self.leader_ports = self.port
                    reset_thread = threading.Thread(target=self.reset_timer)
                    self.iteration += 1
                    reset_thread.name=f"Thread-{self.iteration}"
                    reset_thread.start()
                self.vote = {}
            if self.role == 'leader':
                # Reset election_timer as long as being a leader
                self.election_timer = 0
    
    def sending_procedure(self):

        leader_timer = 0.0
        while self.send_thread_flag:
            if self.role == 'leader':
                time.sleep(0.1)
                leader_timer = round(leader_timer + 0.1, 1)
                if round(leader_timer % self.heartbeat_duration,1) == 0.0:
                    # Send to all nodes every heartbeat duration
                    default_port = self.port - self.node_id
                    for i in range (1,5):
                        message_dict = {}
                        message_dict['sender'] = self.node_id
                        message_dict['term'] = self.term
                        message_dict['message'] = 'log_request'
                        message_dict['ports'] = self.port
                        message_dict['sender_port'] = self.port
                        message_dict['logs'] = self.logs
                        UdpSocket.send(json.dumps(message_dict), default_port+i)
            else:
                leader_timer = 0.0
    
    def receive_log(self, dict_json):
        # Function to process log replication
        reset_thread = threading.Thread(target=self.reset_timer)
        self.iteration += 1
        reset_thread.name=f"Thread-{self.iteration}"
        reset_thread.start()
        self.leader_ports = dict_json['ports']
        self.term = dict_json['term']
        self.logs = dict_json['logs']
        if dict_json['ports'] not in self.alive_ports:
            self.alive_ports.append(dict_json['ports'])

        # Send return message to the Leader
        message_dict={}
        message_dict['sender'] = self.node_id
        message_dict['term'] = self.term
        message_dict['ports'] = self.port
        message_dict['message'] = 'log replicated'
        UdpSocket.send(json.dumps(message_dict), dict_json['sender_port'])

                    
    def listening_procedure(self):
        node_socket = UdpSocket(self.port)
        while self.listen_thread_flag:
            message, address = node_socket.listen()
            dict_json = json.loads(message)

            if self.role == 'follower':
                # Logic if follower get vote request message
                if dict_json['message'] == 'vote_request':
                    if dict_json['term'] > self.term:
                        self.term = dict_json['term']

                        message_dict = {}
                        message_dict['sender'] = self.node_id
                        message_dict['term'] = self.term
                        message_dict['ports'] = self.port
                        message_dict['message'] = 'agree vote'
                        
                        reset_thread = threading.Thread(target=self.reset_timer)
                        self.iteration += 1
                        reset_thread.name=f"Thread-{self.iteration}"
                        reset_thread.start()

                        self.vote[dict_json['term']] = dict_json['sender']
                        UdpSocket.send(json.dumps(message_dict), dict_json['sender_port'])
                # Logic if follower get log request message
                elif dict_json['message'] == 'log_request':
                    receive_log_thread = threading.Thread(target=self.receive_log, args=(dict_json,))
                    receive_log_thread.name="receive_log->election_timer"
                    receive_log_thread.start()
                    
                    
            elif self.role == 'candidate':
                # Logic if candidate got agree vote from other follower
                if dict_json['message'] == 'agree vote':
                    self.voter.append(dict_json['sender'])
                # Logic if candidate got message from a leader
                elif dict_json['message'] == 'log_request':
                    if dict_json['term'] >= self.term:
                        self.term = dict_json['term']
                        self.leader_ports = dict_json['ports']
                        if dict_json['ports'] not in self.alive_ports:
                            self.alive_ports.append(dict_json['ports'])
                        self.role = 'follower'
                        reset_thread = threading.Thread(target=self.reset_timer)
                        self.iteration += 1
                        reset_thread.name=f"Thread-{self.iteration}"
                        reset_thread.start()

    def start(self):
        self.timer_thread = threading.Thread(target=self.election_timer_procedure)
        self.timer_thread.name="start->election_timer"
        self.timer_thread_flag = True
        self.timer_thread.start()

        self.listen_thread = threading.Thread(target=self.listening_procedure)
        self.listen_thread.name="listen_procedure_thread"
        self.listen_thread_flag = True
        self.listen_thread.start()

        self.send_thread = threading.Thread(target=self.sending_procedure)
        self.send_thread_flag = True
        self.send_thread.start()
    
    def stop(self):
        logging.info(f'Node {self.node_id} Stopped')
        self.timer_thread_flag = False
        self.timer_thread.join()

        self.listen_thread_flag = False
        self.listen_thread.join()

        self.send_thread_flag = False
        self.send_thread.join()
        self.stop_signal.set()
    
    def read(self,key):
        logging.info(f"Read Query: {key} value is {self.logs[key]}")
    
    def write(self, key, value):
        self.logs[key] = value
        logging.info(f"Write query: {key} saved with value {value}")
    
    def restart(self):
        print(f"Node {self.node_id}: Restarted")
        self.stop_signal.clear()
        threading.Thread(target=self.start).start()
