import time
import random
import threading

class RaftNode:
    def __init__(self, node_id, total_nodes, leader_id_callback, leader_callback):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.state = 'follower'
        self.leader_id_callback = leader_id_callback
        self.leader_callback = leader_callback
        self.next_index = [0] * self.total_nodes
        self.match_index = [-1] * self.total_nodes
        self.stop_signal = threading.Event()
        self.query_queue = []
        self.query_lock = threading.Lock()

    def run(self):
        while not self.stop_signal.is_set():
            if self.state == 'follower':
                self.follower_behavior()
            elif self.state == 'candidate':
                self.candidate_behavior()
            elif self.state == 'leader':
                self.leader_behavior()
            time.sleep(random.uniform(0.1, 0.2))

    def stop(self):
        self.stop_signal.set()

    def follower_behavior(self):
        print(f"Node {self.node_id}: Follower")
        # Implement follower behavior here

    def candidate_behavior(self):
        print(f"Node {self.node_id}: Candidate")
        # Implement candidate behavior here

    def leader_behavior(self):
        print(f"Node {self.node_id}: Leader")
        with self.query_lock:
            while self.query_queue:
                query = self.query_queue.pop(0)
                self.handle_query(query)

    def add_query(self, query):
        with self.query_lock:
            self.query_queue.append(query)

    def handle_query(self, query):
        query_parts = query.strip().split()
        operation = query_parts[0]

        if operation == 'read':
            key = query_parts[1]
            self.read(key)
        elif operation == 'write':
            key = query_parts[1]
            value = query_parts[2]
            self.write(key, value)
        else:
            print(f"Node {self.node_id}: Invalid operation")

    def read(self, key):
        print(f"Node {self.node_id}: Read operation - Key: {key}")
        # Implement read operation here

    def write(self, key, value):
        print(f"Node {self.node_id}: Write operation - Key: {key}, Value: {value}")
        # Implement write operation here

    def leader_changed_handler(self, new_leader_id):
        self.leader_id_callback(new_leader_id)
        self.leader_callback(self.node_id)

def update_leader_id(new_leader_id):
    global leader_id
    leader_id = new_leader_id

def update_leader(new_leader):
    global leader
    leader = new_leader

if __name__ == '__main__':
    total_nodes = 5  # Change this to the desired number of nodes
    nodes = []
    threads = []

    leader_id = random.randint(0, total_nodes - 1)  # Randomly select the initial leader node
    leader = None

    for i in range(total_nodes):
        node = RaftNode(i, total_nodes, update_leader_id, update_leader)
        nodes.append(node)
        thread = threading.Thread(target=node.run)
        threads.append(thread)
        thread.start()

    # Input queries from a file
    filename = 'queries.txt'  # Specify the file name with your queries
    with open(filename, 'r') as file:
        queries = file.readlines()

    # Assign queries to the leader node
    leader.add_query(" ".join(queries))

    time.sleep(5)  # Run the nodes for another 5 seconds (adjust as needed)

    for node in nodes:
        node.stop()

    for thread in threads:
        thread.join()
