import logging
import random
import sys
import time
import threading
from argparse import ArgumentParser
from quorum_2 import Quorum

# RUN IN PYTHON 3.8.8

list_nodes = []
threads = []
leader_id = 0
leader = None

logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

def update_leader_id(new_leader_id):
    global leader_id
    leader_id = new_leader_id
    global leader
    leader = list_nodes[leader_id]

def reload_logging_config_node(filename):
    from importlib import reload
    reload(logging)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=f"logs/{filename}",
                        filemode='w',
                        level=logging.DEBUG)

def main():
    logger.info("The main program is running...")
    logger.info("Determining the ports that will be used...")
    starting_port = random.randint(10000, 11000)
    number_of_nodes = 5
    port_used = [port for port in range(starting_port, starting_port + number_of_nodes)]


    reload_logging_config_node(f"run.txt")
    for node_id in range(number_of_nodes):
        nodes = Quorum(node_id, port_used[node_id-1], port_used, 1, False, 1, update_leader_id)
        list_nodes.append(nodes)
        thread = threading.Thread(target=nodes.start)
        threads.append(thread)
        thread.start()

    time.sleep(10)

    filename = 'queries/worst/quorum/query_1.txt'
    with open(filename, 'r') as file:
        for i, message in enumerate(file.readlines()):
            time.sleep(1)
            message_list = message.strip().split("-")
            if 'read' in message:
                leader.read()
            elif 'write' in message:
                leader.write()
            elif 'kill' in message:
                list_nodes[message_list[1]].stop()

                if message_list[1] == leader_id:

                    time.sleep(10)
            elif 'restart' in message:
                list_nodes[message_list[1]].restart()
            elif 'end' in message:
                logger.info("Masuk Sini")
                for node in list_nodes:
                    node.stop()
                
                for thread in threads:
                    thread.join()
                
                sys.exit("System Stopped")
             
if __name__ == '__main__':
    main()