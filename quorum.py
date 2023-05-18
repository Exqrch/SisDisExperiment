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
                 heartbeat_duration: float):
        
        # Initiate Needed Variable
        logging.info(f"Initialise port and node dictionary...")
        self.port = port
        self.neighbors_ports = neighbors_ports
        self.new_neighbors_ports = neighbors_ports
        self.node_id = node_id
        
        logging.info(f"Initialise persistent variable...")
        self.heartbeat_duration = heartbeat_duration

        logging.info(f"Initialise volatile state...")
        self.term = 0
        self.vote = {}
        self.election_timer = round(random.uniform(lb_fault_duration, lb_fault_duration+4),1)
        self.alive_ports = neighbors_ports
        self.voter = []
        self.iteration = 2

        logging.info(f"Initialise flag variable...")
        self.role = 'follower'
        self.leader_ports = 0
        self.logs = {}

        logging.info(f"Initialise lock variable...")
        self.is_continue = is_continue
        self.lb_fault_duration = lb_fault_duration

        logging.info(f"Initialise election timer thread...")
        self.timer_thread = threading.Thread(target=self.election_timer_procedure)
        self.timer_thread.name="start->election_timer"

    def reset_timer(self):
        # Function to reset timer with new thread every reset
        self.election_timer = round(random.uniform(self.lb_fault_duration, self.lb_fault_duration+4),1)
        logging.info("Election timer will start...")

    def election_timer_procedure(self):
        # Initiate timer for election
        logging.info("Election timer will start...")
        logging.info(f"Election timer duration: {self.election_timer}s")

        while True:
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
                    logging.info("self.is_stop_timer is True, stop election timer...")
                    reset_thread = threading.Thread(target=self.reset_timer)
                    self.iteration += 1
                    reset_thread.name=f"Thread-{self.iteration}"
                    reset_thread.start()
                    logging.info("Election timer will start...")
                    logging.info(f"Election timer duration: {self.election_timer}s")
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
                    self.leader_ports = self.port
                    reset_thread = threading.Thread(target=self.reset_timer)
                    self.iteration += 1
                    reset_thread.name=f"Thread-{self.iteration}"
                    reset_thread.start()
                    logging.info("Election timer will start...")
                    logging.info(f"Election timer duration: {self.election_timer}s")
                self.vote = {}
            if self.role == 'leader':
                # Reset election_timer as long as being a leader
                self.election_timer = 0
    
    def sending_procedure(self):

        leader_timer = 0.0
        while True:
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
                        UdpSocket.send(json.dumps(message_dict), default_port+i)
            else:
                leader_timer = 0.0
    
    def receive_log(self, dict_json):
        # Function to process log replication
        logging.info("self.is_stop_timer is True, stop election timer...")
        reset_thread = threading.Thread(target=self.reset_timer)
        self.iteration += 1
        reset_thread.name=f"Thread-{self.iteration}"
        reset_thread.start()
        logging.info(f"Election timer duration: {self.election_timer}s")
        self.leader_ports = dict_json['ports']
        self.term = dict_json['term']
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
        while True:
            message, address = node_socket.listen()
            dict_json = json.loads(message)

            if self.role == 'follower':
                # Logic if follower get vote request message
                if dict_json['message'] == 'vote_request':
                    logging.info(f"node{dict_json['sender']} sends a vote_request")
                    logging.info(f"vote procedure is starting...")
                    if dict_json['term'] > self.term:
                        logging.info(f"Candidate {dict_json['sender']} has higher term than my term")
                        logging.info(f"Change term from {self.term} to {dict_json['term']}")
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
                    logging.info(f"Connection for vote_procedure from candidate {dict_json['sender']} has been closed...")
                # Logic if follower get log request message
                elif dict_json['message'] == 'log_request':
                    logging.info(f"node{dict_json['sender']} sends a log_request")
                    logging.info(f"Receive log is starting...")
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
        # TODO
        logging.info("Execute self.candidate_timer_thread.start()...")
        self.timer_thread.start()

        logging.info("Listen for any inputs...")
        listen_thread = threading.Thread(target=self.listening_procedure)
        listen_thread.name="listen_procedure_thread"
        listen_thread.start()

        send_thread = threading.Thread(target=self.sending_procedure())
        send_thread.start()

        pass


def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.INFO)

def main(heartbeat_duration=1, lb_fault_duration=1, port=1000,
         node_id=1, neighbors_ports=(1000,), is_continue=False):
    reload_logging_windows(f"logs/node{node_id}.txt")
    threading.excepthook = thread_exception_handler
    try:
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"lower_bound_fault_duration: {lb_fault_duration}")
        logging.debug(f"upper_bound_fault_duration = {lb_fault_duration}s + 4s")
        logging.debug(f"port: {port}")
        logging.debug(f"neighbors_ports: {neighbors_ports}")
        logging.debug(f"is_continue: {is_continue}")

        logging.info("Create raft object...")
        raft = Raft(node_id, port, neighbors_ports, lb_fault_duration, is_continue, heartbeat_duration)

        logging.info("Execute raft.start()...")
        raft.start()
    except Exception:
        logging.exception("Caught Error")
        raise
