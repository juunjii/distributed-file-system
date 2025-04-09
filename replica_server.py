import sys
import os
import glob
import socket

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# from coordinator import coordinator
# from dfs import replica
# from dfs.ttypes import *


class ReplicaHandler:
    def __init__(self, local_dir, is_coordinator, nodes, quorum_size):
        # Local directory
        self.local_dir = local_dir
        self.is_coordinator = is_coordinator
        
        # List storing tuples of replica server - (host, port, coordinator flag)
        self.nodes = nodes
        # Stores number of replica servers, Nr and Nw
        self.quorum_size = quorum_size

        # {fname: version}
        self.file_version = {}

       
        

'''
Parse list of replica severs from compute_nodes.txt
Gets compute nodes' respective ip, port, flag 
'''
def parse_compute_nodes(self):
    nodes = []  # List storing tuples of replica server - (host, port, coordinator flag)
    quorum_size = []  # Stores number of replica servers, Nr and Nw
    try:
        with open('compute_nodes.txt', 'r') as file:
            # Get size of quorums Nr, Rw
            quorum = file.readline().strip()
            nr, nw = map(int, quorum.split(','))
            quorum_size.append((nr,nw))

            for line in file:
                host, port, is_coordinator = line.strip().split(',')
                nodes.append((host, int(port), int(is_coordinator)))

    except Exception as e:
        print(f"Error parsing compute nodes: {e}")
        sys.exit(1)
    
    return quorum_size, nodes

'''
Checks if local directory exist, 
else creates it 
'''
def check_directory(dir_path):
    if dir_path is None:
        return -1
    try: 
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"Directory '{dir_path}' created.")
        else:
            print(f"Directory '{dir_path}' already exists.")
    except Exception as e: 
        print(f"An error occurred while checking or creating the directory: {e}")
        sys.exit(1)

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Connect to an external IP (doesn't have to be reachable)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"  # fallback
    finally:
        s.close()
    return ip

print("Local IP Address:", get_local_ip())

def main():
    if len(sys.argv) < 3:
        print("Usage: ./replica_server.py <local_directory> <compute_nodes_file> <port>")
        sys.exit(1)

    dir = sys.argv[1]
    config = sys.argv[2]
    port = sys.argv[3]


    dir_check = check_directory(dir)
    if dir_check == 1:
        print(f"Error creating directory...")


    quorum_size, nodes = parse_compute_nodes(config)

    local_ip = get_local_ip()

    # TODO: compare local ip and port to compute_nodes to see whether you are coordinator 



    # handler = ReplicaHandler(dir, )




if __name__ == "__main__":
    # get_local_ip()

    