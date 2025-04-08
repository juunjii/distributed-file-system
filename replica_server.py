import sys

import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# from coordinator import coordinator
from dfs import replica
from dfs.ttypes import *


class ReplicaHandler:
    def __init__(self, local_dir, is_coordinator):
        # Local directory
        self.local_dir = local_dir
        self.is_coordinator = is_coordinator

        self.nodes = []
        # Stores number of replica servers, Nr and Nw
        self.quorum_size = [] 
        # {fname: version}
        self.file_version = {}


        self.parse_compute_nodes()
        print(self.parse_compute_nodes)

    '''
    Parse list of severs from compute_nodes.txt
    Gets compute nodes' respective ip, port, flag 
    '''
    def parse_compute_nodes(self):
        try:
            with open('compute_nodes.txt', 'r') as file:
                # Get size of quorums Nr, Rw
                quorum = file.readline().strip()
                nr, nw = map(int, quorum.split(','))
                self.quorum_size.append((nr,nw))

                for line in file:
                    host, port, is_coordinator = line.strip().split(',')
                    self.nodes.append((host, int(port), int(is_coordinator)))

        except Exception as e:
            print(f"Error parsing compute nodes: {e}")
            sys.exit(1)

