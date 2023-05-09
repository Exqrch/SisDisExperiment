from network import Node
import random
import time

global status_dictionary
global id_dictionary
status_dictionary={}
id_dictionary = {}
# Quorum Node Implemented using Gossip Protocol
# Algorithm adapted from Assignment 2 Distributed System
class QuorumNode(Node):

    def __init__(self, network, node_id, node_address):
        super().__init__(network, node_id, node_address)

        self.leader_id = None
        self.is_leader = False

    def start_fault_timer(key):
        status_dictionary[key][1] = False
    
    def send_message(dest_id):
        message = f""
        message = message.encode("UTF-8")
        Node.send_to(dest_id, message)
    
    def sending_procedure(heartbeat_duration, node_id, neighbors_id):
        while True:
            status_dictionary[f"node-{node_id}"][0] += 1

            id_1 = random.choice(neighbors_id)
            id_2 = random.choice(neighbors_id)
            while id_1 == id_2:
                id_2 = random.choice(neighbors_id)
            send_message(id_1)
            send_message(id_2)
            time.sleep(heartbeat_duration)
    
    def fault_timer_procedure(node_id, fault_duration):
        for key in status_dictionary.keys():
            if key != node_id:
                thread = threading.Timer(fault_duration, start_fault_timer, (key,))
                thread.start()
