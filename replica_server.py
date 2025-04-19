from queue import Queue
import random
import sys
import os
import glob
import socket
import threading

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# from coordinator import coordinator
from dfs import ReplicaService

CHUNK_SIZE = 2048

class ReplicaHandler:
    def __init__(self, local_dir, is_coordinator = 0, nodes = None, quorum_size = None):
        # Local directory
        self.local_dir = local_dir
        self.is_coordinator = is_coordinator
        
        # List storing tuples of replica server - (host, port, coordinator flag)
        self.nodes = nodes
        
        # For phase 2
        self.nr = quorum_size[0][0] # Replicas in read quorum
        self.nw = quorum_size[0][1] # Replicas in write quorum

        # {fname: version}
        self.file_version = {}
        self.lock = threading.Lock()
        self.requests = Queue()

        # Starts processing client request if coordinator 
        if is_coordinator:
            thread = threading.Thread(target=self.thread_func)
            thread.daemon = True
            thread.start()

    '''Run by the coordinator on startup'''
    def thread_func(self):
        while True:
            request = self.requests.get()
            with self.lock:
                if request["t"] == "r":
                    self.coord_read(request)
                else:
                    self.coord_write(request)
            self.requests.task_done()

    '''Get the file version, 0 if filename doesn't exist'''
    def get_versionnum(self, fname):
        return self.file_version.get(fname, 0)
    
    '''Set file version'''
    def set_versionnum(self, fname, version):
        self.file_version[fname] = version

    '''
    Gets size of file 
    '''
    def get_file_size(self, fname):
        path = os.path.join(self.local_dir, fname)
        return os.path.getsize(path)
    
    '''Called by coordintaor, replicates data into local file directory'''
    def replicate(self, fname, data, version):
        path = os.path.join(self.local_dir, fname) 
        with open(path, 'wb') as f:
            f.write(data)
        self.set_versionnum(fname, version)
    
    '''
    Gets CHUNK_SIZE amount of bytes from a file 
    '''
    def get_file_chunk(self, filename, offset, chunk_size):
        path = os.path.join(self.local_dir, filename)
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(chunk_size)

    '''
    Connect to a replica to get latest version of file 
    '''
    def connect_to_replica(self, ip, port):
        try:
            transport = TSocket.TSocket(ip, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ReplicaService.Client(protocol)
            transport.open()
            return client, transport  
        except Exception as e:
            print(f"Failed to connect to node {ip}:{port} - {e}")
            return None, None  
    
    '''
    Copy file from another replica into local directory
    '''
    def request_file(self, fname, ip, port):
        client, transport = self.connect_to_replica(ip, port)
        if client is not None and transport is not None:
            try: 
                size = client.get_file_size(fname)
                with open(os.path.join(self.local_dir, fname), "wb") as f:
                    offset = 0
                    while offset < size:
                        # Gets 2048 bytes from file 
                        chunk = client.get_file_chunk(fname, offset, CHUNK_SIZE)
                        f.write(chunk)
                        offset += len(chunk)
            except Exception as e:
                print(f"Error requesting copy of file {fname} from {ip}:{port} - {e}")
                return -1

            finally: 
                transport.close()


    '''Called by client/coordinator, this code handles read requests'''
    def manage_read(self, fname):
        if not self.is_coordinator:
            for ip, port, is_coord in self.nodes:
                if is_coord == 1:
                    client, transport = self.connect_to_replica(ip, port)
                    if client and transport:
                        try:
                            return client.manage_read(fname)
                        finally:
                            transport.close()
        else:
            self.requests.put({"t": 'r', 'fname': fname})

    '''Called by client/coordinator, this code handles write requests'''
    def manage_write(self, fname, data):
        if not self.is_coordinator:
            for ip, port, is_coord in self.nodes:
                if is_coord == 1:
                    client, transport = self.connect_to_replica(ip, port)
                    if client and transport:
                        try:
                            return client.manage_write(fname, data)
                        finally:
                            transport.close()
        else:
            self.requests.put({"t": 'w', 'fname': fname, 'data': data})
        return 0

    '''Coordintator's read function, randomly fetches nr nodes to read from'''
    def coord_read(self, request):
        q = random.sample(self.nodes, self.nr)
        versions = []
        for ip, port, i in q:
            client, transport = self.connect_to_replica(ip, port)
            if client and transport:
                try:
                    versions.append((client.get_versionnum(request["fname"]), ip, port))
                finally:
                    transport.close()
        if len(versions) == 0:
            return
        max_version, ip, port = max(versions)
        local = self.get_versionnum(request["fname"])
        if local < max_version:
            # Local file is out of date
            self.request_file(request["fname"], ip, port)

    '''Coordinator's write function, randomly grabs nw nodes to write'''
    def coord_write(self, request):
        fname = request['fname']
        data = request['data']
        q = random.sample(self.nodes, self.nw)
        versions = []
        for ip, port, i in q:
            client, transport = self.connect_to_replica(ip, port)
            if client and transport:
                try:
                    # Gather every node's file version
                    versions.append((client.get_versionnum(request["fname"]), ip, port))
                finally:
                    transport.close()

        # Get the max of the existing file versions and update everyone to the next version
        version_nums = [v[0] for v in versions]
        max_ver = max(version_nums + [0]) + 1

        for ip, port, _ in q:
            if ip != get_local_ip() or port != int(sys.argv[3]):
                client, transport = self.connect_to_replica(ip, port)
                if client and transport:
                    try:
                        # Make the replicas copy the files down
                        client.replicate(fname, data, max_ver)
                    finally:
                        transport.close()
            else:
                self.replicate(fname, data, max_ver)
            
    '''Get;s local files with versions'''
    def get_local_files(self):
        return self.file_version

    '''Called by client to list all files, forwards to replica which does the work of gathering all the file info'''
    def list_files(self):
        if not self.is_coordinator:
            for ip, port, is_coord in self.nodes:
                if is_coord == 1:
                    client, transport = self.connect_to_replica(ip, port)
                    if client and transport:
                        try:
                            return client.list_files()
                        finally:
                            transport.close()
        else:
            files = {}
            for ip, port, _ in self.nodes:
                client, transport = self.connect_to_replica(ip, port)
                if client and transport:
                    try:
                        files = client.get_local_files()
                        for fname, version in files.items():
                            if fname not in files or version > files[fname]:
                                files[fname] = version
                    finally:
                        transport.close()
            return files

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

'''
Get ip of current running replica server 
'''
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

def main():
    if len(sys.argv) != 4:
        print("Usage: ./replica_server.py <local_directory> <compute_nodes_file> <port>")
        sys.exit(1)

    dir = sys.argv[1]
    config = sys.argv[2]
    port = int(sys.argv[3])

    dir_check = check_directory(dir)
    if dir_check == 1:
        print(f"Error creating directory...")


    quorum_size, nodes = parse_compute_nodes(config)

    # print(nodes)

    local_ip = get_local_ip()
    print(local_ip)
    
    is_coordinator = 0

    # # Find port based on ip and get coordinator flag
    for ip, node_port, coordinator_flag in nodes:
        if ip == local_ip and port==node_port:
            # port = node_port 
            is_coordinator = coordinator_flag
            break
    
    # if port is None:
    #     print("Error: Could not determine port from compute_nodes.txt")
    #     sys.exit(1)
    
    handler = ReplicaHandler(dir, is_coordinator, nodes, quorum_size)


    # Create server
    processor = ReplicaService.Processor(handler)
    transport = TSocket.TServerSocket(host="0.0.0.0", port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    
    print(f"Starting replica server on port {port}")
    print(f"Local directory: {dir}")
    print(f"Is coordinator: {is_coordinator}")
    
    try:
        server.serve()
    except KeyboardInterrupt:
        print("Shutting down")


if __name__ == "__main__":
    main()

