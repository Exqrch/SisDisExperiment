from vsr_2 import Client, Replica
from network import Network
import threading
import time
import logging
import multiprocessing

def main():


    ip = "127.0.0.1"
    node_ids = [0, 1, 2, 3, 4]
    node_addresses = [(ip, port) for port in [5001, 5002, 5003, 5004, 5005]]
    client_addr =  ("127.0.0.1", 5000)
    
    scenario = "best"
    directory = f'queries/{scenario}/vsr/query_1.txt'

    network = Network(node_ids, node_addresses)

    f = 1
    quorumsize = f + 1
    
    processes = []
    
    for i in node_ids:
        run_process = multiprocessing.Process(target=start_replica, args=(network, i, node_addresses[i], quorumsize, 1))
        run_process.name = f"run_process_replica_{i}"
        processes.append(run_process)
        run_process.start()


    client_process = multiprocessing.Process(target=start_client, args=(node_addresses, 30, client_addr, scenario))
    client_process.name = "client_process"
    processes.append(client_process)
    client_process.run()
    


def start_replica(network, id, address, quorumsize, timeout):
    replica = Replica(network, id, address, quorumsize, timeout, ("127.0.0.1", 5000))
    replica.run()
 
def start_client(node_addresses, timeout, client_addr, scenario):
    client = Client(node_addresses, timeout, client_addr, scenario)
    client.run()

if __name__ == "__main__":
    main()
