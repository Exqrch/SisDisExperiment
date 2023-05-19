from vsr import Client, Replica
from network import Network
import threading
import time


def main():
    timer = time.monotonic_ns()

    ip = "127.0.0.1"
    node_ids = [0, 1, 2, 3, 4]
    node_addresses = [(ip, port) for port in [5001, 5002, 5003, 5004, 5005]]
    clients = {5000: (ip, 5000)}

    network = Network(node_ids, node_addresses)

    f = 1
    quorumsize = f + 1

    replica_1 = Replica(network, 0, node_addresses[0], quorumsize, 100, clients)
    replica_2 = Replica(network, 1, node_addresses[1], quorumsize, 100, clients)
    replica_3 = Replica(network, 2, node_addresses[2], quorumsize, 100, clients)
    replica_4 = Replica(network, 3, node_addresses[3], quorumsize, 100, clients)
    replica_5 = Replica(network, 4, node_addresses[4], quorumsize, 100, clients)

    replicas = [replica_1, replica_2, replica_3, replica_4, replica_5]

    for replica in replicas:
        listen_thread = threading.Thread(target=replica.listen)
        listen_thread.name = f"listen_thread_{replica.node_id}"
        listen_thread.start()

        run_thread = threading.Thread(target=replica.run)
        run_thread.name = f"run_thread_{replica.node_id}"
        run_thread.start()

    print("All replicas are listening")

    client = Client(node_addresses, 10, ip, 5000)
    listen_thread = threading.Thread(target=client.listen)
    listen_thread.name = f"client_listen_thread"
    listen_thread.start()

    client.run()
    print("TIME TAKEN", time.monotonic_ns() - timer)


if __name__ == "__main__":
    main()
