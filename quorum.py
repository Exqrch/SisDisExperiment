from network import Node
import socket
from pprint import pformat
import random
import time
import threading
import logging

# Quorum Node Implemented using Gossip Protocol
# Algorithm adapted from Assignment 2 Distributed System
class QuorumNode(Node):

    def __init__(self, network, node_id, node_address):
        super().__init__(network, node_id, node_address)

        self.leader_id = None
        self.is_leader = False

        global status_dictionary
        global id_dictionary
        global logger

        status_dictionary={}
        id_dictionary = {}
        logger = logging.getLogger(__name__)


    def start_fault_timer(key):
        status_dictionary[key][1] = False
        logger.info(f"This node become a fault: {key}")
        logger.info(f"Node fault status_dictionary:\n{pformat(status_dictionary)}")


    def send_message(node_id, port):
        logger.debug("Create the client socket...")
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logger.debug("Encode the message...")
        message = f"node-{node_id}#{status_dictionary}"
        logger.debug(f"message: {message}")
        message = message.encode("UTF-8")
        addr = ("127.0.0.1", port)
        logger.debug("Send the message...")
        client_socket.sendto(message, addr)


    def sending_procedure(self, heartbeat_duration, node_id, neighbors_port, node_ports):
        while True:
            status_dictionary[f"node-{node_id}"][0] += 1
            logger.info(f"Increase heartbeat node-{node_id}:\n{pformat(status_dictionary)}")

            logger.info("Determining which node to send...")
            port_1 = random.choice(neighbors_port)
            port_2 = random.choice(neighbors_port)
            while port_1 == port_2:
                port_2 = random.choice(neighbors_port)
            logger.info(f"Send messages to node-{node_ports[port_1]} and node-{node_ports[port_2]}")
            self.send_message(node_id, port_1)
            self.send_message(node_id, port_2)
            time.sleep(heartbeat_duration)


    def fault_timer_procedure(self, node_id, fault_duration):
        for key in status_dictionary.keys():
            logger.debug(f"key: {key}")
            if key == node_id:
                continue
            thread = threading.Timer(fault_duration, self.start_fault_timer, (key,))
            thread.start()


    def listening_procedure(self, port, node_id, fault_duration):
        logger.info("Start the timer for fault duration...")
        thread_dictionary = {}
        for key in status_dictionary.keys():
            logger.debug(f"key: {key}")
            if key == f"node-{node_id}":
                continue
            thread = threading.Timer(fault_duration, self.start_fault_timer, (key,))
            thread.start()
            thread_dictionary[key] = thread


        logger.info("Initiating socket...")
        BUFFER_SIZE = 1024
        LOCAL_IP = "127.0.0.1"
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sc:
            sc.bind((LOCAL_IP, port))

            logger.info("Listen for incoming messages...")
            while True:
                message_byte, address = sc.recvfrom(BUFFER_SIZE)
                message_raw = message_byte.decode("UTF-8")
                logger.debug(f"message_raw: {message_raw}")
                source = message_raw.split("#")[0]
                message = message_raw.split("#")[1]
                logger.info(f"Receive message from {source}...")
                input_status_dictionary: dict = self.literal_eval(message)
                logger.info(f"Incoming message:\n{pformat(input_status_dictionary)}")
                logger.debug(f"address: {address}")

                logger.debug("Process the message...")
                for key in input_status_dictionary.keys():
                    input_node_list = input_status_dictionary[key]
                    current_node_list = status_dictionary[key]
                    logger.debug(f"input_node_list {key}: {input_node_list}")
                    logger.debug(f"current_node_list {key}: {current_node_list}")
                    if input_node_list[0] <= current_node_list[0]:
                        logger.debug("Skip this loop...")
                        continue

                    logger.debug(f"Update logical time {key}: {current_node_list[0]} -> {input_node_list[0]}")
                    logger.debug(f"Check if {key} has died...")
                    current_node_list[0] = input_node_list[0]
                    if input_node_list[1]:
                        logger.debug(f"{key} has not died...")

                        logger.debug(f"Restart {key} thread...")
                        thread_dictionary[key].cancel()
                        current_node_list[1] = True
                        thread = threading.Timer(fault_duration, self.start_fault_timer, (key,))
                        thread_dictionary[key] = thread
                        thread.start()
                        continue
                    logger.debug(f"{key} has died...")
                logger.info(f"status_dictionary after incoming message:\n{pformat(status_dictionary)}")

    def handle_exception(exc_type, exc_value, exc_traceback):
        logger.error(f"Uncaught exception HAHAHA", exc_info=(exc_type, exc_value, exc_traceback))

    def thread_exception_handler(args):
        logger.error(f"Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

    def reload_logging_windows(filename):
        log = logging.getLogger()
        for handler in log.handlers:
            log.removeHandler(handler)
        logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                            datefmt='%H:%M:%S',
                            filename=filename,
                            filemode='w',
                            level=logging.INFO)

    def main(self, heartbeat_duration=1, num_of_neighbors_to_choose=1,
            fault_duration=1, port=1000, node_id=1, neighbors_ports=(1000,)):
        self.reload_logging_windows(f"logs/node{node_id}.txt")
        global logger
        logger = logging.getLogger(__name__)
        try:
            logger.info(f"Node with id {node_id} is running...")
            logger.debug(f"heartbeat_duration: {heartbeat_duration}")
            logger.debug(f"fault_duration: {fault_duration}")
            logger.debug(f"port: {port}")
            logger.debug(f"num_of_neighbors_to_choose: {num_of_neighbors_to_choose}")
            logger.debug(f"neighbors_ports: {neighbors_ports}")

            logger.info("Configure the status_dictionary global variable...")
            global status_dictionary
            status_dictionary = {}
            node_ports = {}
            for i in range(len(neighbors_ports)):
                status_dictionary[f"node-{i + 1}"] = [0, True]
                node_ports[neighbors_ports[i]] = i+1
            neighbors_ports.remove(port)
            logger.info(f"status_dictionary:\n{pformat(status_dictionary)}")
            logger.info("Done configuring the status_dictionary...")

            logger.info("Executing the listening procedure...")
            threading.excepthook = self.thread_exception_handler
            thread = threading.Thread(target=self.listening_procedure, args=(port, node_id, fault_duration))
            thread.name = "listening_thread"
            thread.start()
            logger.info("Executing the sending procedure...")
            thread = threading.Thread(target=self.sending_procedure,
                            args=(heartbeat_duration,
                                node_id, neighbors_ports, node_ports))
            thread.name = "sending_thread"
            thread.start()
        except Exception as e:
            logger.exception("Caught Error")
            raise
