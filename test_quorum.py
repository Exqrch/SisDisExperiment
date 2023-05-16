import logging
import multiprocessing
import random
import sys
import time
from argparse import ArgumentParser
import quorum

# RUN IN PYTHON 3.8.8

logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

class NodeProcess(multiprocessing.Process):

    def run(self):
        try:
            super().run()
        except Exception:
            logger.error(f"{self.name} has an error")


def reload_logging_config_node(filename):
    from importlib import reload
    reload(logging)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=f"logs/{filename}",
                        filemode='w',
                        level=logging.INFO)

def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error(f"Uncaught exception HAHAHA", exc_info=(exc_type, exc_value, exc_traceback))

def main():
    parser = ArgumentParser()
    parser.add_argument("-n", type=str, dest="node",
                        help="The number of nodes", default="4")
    parser.add_argument("-m", type=str, dest="neighbors",
                        help="The number of chosen neighbors when a node wants to send a gossip message", default=2)
    parser.add_argument("-b", type=str, dest="heartbeat",
                        help="The particular duration of the heartbeat", default=1)
    parser.add_argument("-f", type=str, dest="fault_duration",
                        help="The particular duration to assume a node to be a fault", default=2)
    parser.add_argument("-p", type=str, dest="port",
                        help="Starting port", default=6574)
    parser.add_argument("-d", type=str, dest="kill_duration",
                        help="The particular duration for a node "
                             "to become a fault", default=3)
    args = parser.parse_args()

    sys.excepthook = handle_exception

    logger.info("The main program is running...")
    logger.info("Determining the ports that will be used...")
    starting_port = random.randint(10000, 11000)
    number_of_nodes = int(args.node)
    port_used = [port for port in range(starting_port, starting_port+number_of_nodes)]
    logger.debug(f"port_used: {port_used}")
    logger.info("Done determining the ports that will be used...")

    logger.debug(f"number_of_nodes: {number_of_nodes}")
    logger.debug(f"heartbeat: {float(args.heartbeat)}")
    logger.debug(f"fault_duration: {args.fault_duration}")
    list_of_node = []
    logger.info("Start running multiple nodes...")
    for node_id in range(number_of_nodes):
        reload_logging_config_node(f"node{node_id+1}.txt")
        process = NodeProcess(target=quorum.main, args=(
            float(args.heartbeat), int(args.neighbors),
            float(args.fault_duration), starting_port+node_id,
            node_id+1, port_used
        ))
        process.start()
        list_of_node.append(process)
    logger.info("Done running multiple nodes...")
    logger.debug(f"number of running processes: {len(list_of_node)}")


    kill_duration = args.kill_duration
    logger.debug(f"kill_duration: {kill_duration}")
    logger.info("Start stopping the nodes...")
    for node_id in range(number_of_nodes):
        time.sleep(int(kill_duration))
        # node_index = random.randint(0, len(list_of_node)-1)
        # process = list_of_node.pop(node_index)
        # node_index = random.randint(0, len(list_of_node) - 1)
        process = list_of_node.pop()
        process.kill()
        logger.debug(f"Kill process with ID: {process.name}")
    logger.info("Done stopping all the nodes...")


if __name__ == '__main__':
    main()