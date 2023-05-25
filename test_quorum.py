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
start_time = 0
read_queries_time = []
write_queries_time = []
election_time = []
end_time = 0

logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

def update_leader_id(new_leader_id):
    global leader_id
    leader_id = new_leader_id
    global leader
    leader = list_nodes[leader_id]

def record_queries(time, type):
    if type == 'read':
        global read_queries_time
        read_queries_time.append(time)
    if type == 'write':
        global write_queries_time
        write_queries_time.append(time)

def record_election(time):
    global election_time
    election_time.append(time)

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

    global start_time
    start_time = time.monotonic_ns()

    starting_port = random.randint(10000, 11000)
    number_of_nodes = 5
    port_used = [port for port in range(starting_port, starting_port + number_of_nodes)]


    reload_logging_config_node(f"run.txt")
    for node_id in range(number_of_nodes):
        nodes = Quorum(node_id, port_used[node_id-1], port_used, 1, False, 1, update_leader_id, record_election, record_queries)
        list_nodes.append(nodes)
        thread = threading.Thread(target=nodes.start)
        threads.append(thread)
        thread.start()

    query_type = input ("Queries type (best/worst):")
    logger.info("Please wait, the system is searching for their leader")
    time.sleep(10)

    filename = f'queries/{query_type}/quorum/query_1.txt'
    with open(filename, 'r') as file:
        for i, message in enumerate(file.readlines()):
            message_list = message.strip().split("-")
            if 'READ' in message:
                leader.read(message_list[1])
            elif 'SET' in message:
                value = message_list[1].split("=")
                leader.write(value[0], value[1])
            elif 'kill' in message:
                list_nodes[int(message_list[1])].stop()
                logging.info(f'nodes {message_list[1]} killed')

                if message_list[1] == leader_id:
                    time.sleep(10)
                    logging.info(f'nodes {message_list[1]} is a leader')
            elif 'restart' in message:
                list_nodes[int(message_list[1])].restart()
                logging.info(f'nodes {message_list[1]} restarted')
            elif 'end' in message:
                end_time = time.monotonic_ns()
                average_read = 0
                average_write = 0
                average_election = 0

                for times in read_queries_time:
                    average_read += times/1e9
                
                for times in write_queries_time:
                    average_write += times/1e9

                for times in election_time:
                    average_election += times/1e9
                
                average_read = average_read/len(read_queries_time)
                average_write = average_write/len(write_queries_time)
                average_queries = (average_write + average_read)/2
                average_election = average_election/len(election_time)

                logging.info(f"Average Read Queries time: {average_read} from {len(read_queries_time)} queries")
                logging.info(f"Average Write Queries time: {average_write} from {len(write_queries_time)} queries")
                logging.info(f"Average Queries time: {average_queries} from {len(read_queries_time) + len(write_queries_time)} queries")
                logging.info(f"AverageElection time: {average_election} from {len(election_time)} election")
                logging.info(f"Total runtime duration: {(end_time - start_time)/1e9}")
                
                for node in list_nodes:
                    node.stop()
                
                for thread in threads:
                    thread.join()
                
                sys.exit("System Stopped")
             
if __name__ == '__main__':
    main()