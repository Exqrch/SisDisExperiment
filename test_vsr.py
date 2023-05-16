from vsr import Client, Replica
import threading
import asyncio
import nest_asyncio


def listen_in_thread(replica: Replica):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(replica.listen())


def run_in_thread(replica: Replica):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(replica.run())


async def main():
    ip = '127.0.0.1'
    node_addresses = [5001, 5002, 5003, 5004, 5005]

    f = 1
    quorumsize = f + 1

    replica_1 = Replica(node_addresses, quorumsize, 5,
                        dict(), ip, node_addresses[0])
    replica_2 = Replica(node_addresses, quorumsize, 5,
                        dict(), ip, node_addresses[1])
    replica_3 = Replica(node_addresses, quorumsize, 5,
                        dict(), ip, node_addresses[2])
    replica_4 = Replica(node_addresses, quorumsize, 5,
                        dict(), ip, node_addresses[3])
    replica_5 = Replica(node_addresses, quorumsize, 5,
                        dict(), ip, node_addresses[4])

    replicas = [replica_1, replica_2, replica_3, replica_4, replica_5]

    for replica in replicas:
        listen_thread = threading.Thread(
            target=listen_in_thread, args=(replica,))
        listen_thread.start()

        run_thread = threading.Thread(target=run_in_thread, args=(replica,))
        run_thread.start()

    print("All replicas are listening")

    client = Client(node_addresses, 10, ip, 5000)
    await client.run()

nest_asyncio.apply()
asyncio.run(main())
