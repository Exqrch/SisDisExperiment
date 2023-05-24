from vsr_3 import Client, Replica
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

    network = Network(node_ids, node_addresses)

    f = 1
    quorumsize = f + 1
    
    replicas = []
    
    replica_0 = Replica(network, 0, node_addresses[0], quorumsize, 5, client_addr)
    replica_1 = Replica(network, 1, node_addresses[1], quorumsize, 5, client_addr)
    replica_2 = Replica(network, 2, node_addresses[2], quorumsize, 5, client_addr)
    replica_3 = Replica(network, 3, node_addresses[3], quorumsize, 5, client_addr)
    replica_4 = Replica(network, 4, node_addresses[4], quorumsize, 5, client_addr)

    replicas = [replica_0, replica_1, replica_2, replica_3, replica_4]

    client = Client(replicas, 30, client_addr, "worst")
    
    for replica in replicas:
        run_thread = threading.Thread(target=replica.run)
        run_thread.name = f"run_thread_{replica.node_id}"
        replica.add_client_callback(client.callback)
        run_thread.start()

    print("All replicas are listening", time.monotonic_ns())

    client_thread = threading.Thread(target=client.run)
    client_thread.name = f"client_thread"
    client_thread.start()
    
    # for i in node_ids:
        # run_process = multiprocessing.Process(target=start_replica, args=(network, i, node_addresses[i], quorumsize, 1))
        # run_process.name = f"run_process_replica_{i}"
        # run_process.start()

    # print("All replicas are running", time.monotonic_ns() - timer)

    # client_process = multiprocessing.Process(target=start_client, args=(node_addresses, 30, client_addr))
    # client_process.name = "client_process"
    # client_process.run()
    # print("TIME TAKEN", time.monotonic_ns() - timer)

# def start_replica(network, id, address, quorumsize, timeout):
#     replica = Replica(network, id, address, quorumsize, timeout, ("127.0.0.1", 5000))
#     replica.run()
 
# def start_client(node_addresses, timeout, client_addr):
#     client = Client(node_addresses, timeout, client_addr)
#     client.run()

if __name__ == "__main__":
    main()
