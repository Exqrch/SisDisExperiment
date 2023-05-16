import socket
import threading
import select
import asyncio
import time


class Network:
    def __init__(self, node_ids, node_addresses):
        self.nodes = {}
        for node_id, node_address in zip(node_ids, node_addresses):
            ip_address, port = node_address
            self.nodes[int(node_id)] = (ip_address, int(port))

    def relay_to_all(self, sender_id, message):
        for node_id in self.nodes:
            if node_id != sender_id:
                receiver_address = self.nodes[int(node_id)]
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(receiver_address)
                    s.sendall(message.encode('utf-8'))

    def relay(self, sender_id, target_id, message):
        receiver_address = self.nodes[int(target_id)]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(receiver_address)
            s.sendall(message.encode('utf-8'))


class Node:
    def __init__(self, network, node_id, node_address):
        self.network = network
        self.node_id = node_id
        self.address = node_address
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(self.address)

        # For testing purposes
        self.last_message = None

    # Should be overwritten by subclass
    def listen(self):
        self.socket.listen()
        while True:
            try:
                conn, addr = self.socket.accept()
                data = conn.recv(1024)
                self.last_message = data.decode('utf-8')
                conn.close()
            except OSError:
                break

    def broadcast(self, message):
        self.network.relay_to_all(self.node_id, message)

    def send_to(self, target_id, message):
        self.network.relay(self.node_id, target_id, message)

    def start_listening(self):
        threading.Thread(target=self.listen).start()


class AsyncNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    async def listen(self):
        server = await asyncio.start_server(
            self.handle_client, self.ip, self.port)
        async with server:
            print("Node listening on", self.ip, self.port)
            await server.serve_forever()

    def new_loop(self):
        return asyncio.new_event_loop()

    # Should be overwritten by subclass
    async def handle_client(self, reader, writer):
        pass

    async def send(self, port, message):
        reader, writer = await asyncio.open_connection(self.ip, port)
        writer.write(message)
        print("Sending message to", port)
        await writer.drain()
        writer.close()
