from vsr_2 import Client, Replica
from network import Network
import threading
import time
import logging
import multiprocessing

def main():

    timer = time.monotonic_ns()

    ip = "127.0.0.1"
    node_ids = [0, 1, 2, 3, 4]
    node_addresses = [(ip, port) for port in [5001, 5002, 5003, 5004, 5005]]
    client_addr =  ("127.0.0.1", 5000)

    network = Network(node_ids, node_addresses)

    f = 1
    quorumsize = f + 1
    
    for i in node_ids:
        run_process = multiprocessing.Process(target=start_replica, args=(network, i, node_addresses[i], quorumsize, 10))
        run_process.name = f"run_process_replica_{i}"
        run_process.start()

    print("All replicas are running", time.monotonic_ns() - timer)

    client_process = multiprocessing.Process(target=start_client, args=(node_addresses, 30, client_addr))
    client_process.name = "client_process"
    client_process.run()
    print("TIME TAKEN", time.monotonic_ns() - timer)
    
def start_replica(network, id, address, quorumsize, timeout):
    replica = Replica(network, id, address, quorumsize, timeout, ("127.0.0.1", 5000))
    replica.run()
 
def start_client(node_addresses, timeout, client_addr):
    client = Client(node_addresses, timeout, client_addr)
    client.run()

if __name__ == "__main__":
    main()
