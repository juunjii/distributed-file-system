
import os
import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from thrift import Thrift
from thrift.server import TServer
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

# copy_test.py
from dfs import ReplicaService
from dfs.ttypes import Res

from replica_server import ReplicaHandler
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

def connect_to_replica(ip, port):
    try:
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ReplicaService.Client(protocol)
        transport.open()
        return client, transport
    except Exception as e:
        print(f"Failed to connect to replica at {ip}:{port} - {e}")
        return None, None

def main():
    # Trigger the file copy from replica2 (replica running on port 9091)
    # handler = ReplicaHandler("./replica2_data")
    # success = handler.request_file("test.txt", "127.0.0.1", 9090)

    # if success != -1:
    #     print("File copy triggered successfully.")

    if len(sys.argv) < 4:
        print("Usage:")
        print("  python client.py <replica_ip> <replica_port> read <filename>")
        print("  python client.py <replica_ip> <replica_port> write <filepath>")
        print("  python client.py <replica_ip> <replica_port> list")
        sys.exit(1)

    CHUNK_SIZE = 2048
    CLIENT_LOCAL_DIR = "./client_files"


    ip = sys.argv[1]
    port = sys.argv[2]
    operation = sys.argv[3]
    # nodes = parse_compute_nodes(config)
    # coordinator = None
    # for node in nodes:
    #     if int(node[2]) == 1:
    #         coordinator = node
    #         break
    # if not coordinator:
    #     print("Must mark one node as a coordinator in compute_nodes.txt")
    #     sys.exit(1)
        
    client, transport = connect_to_replica(ip, port)
    
    if not client:
        print("error connecting to replica")
        sys.exit(1)

    if operation == "read":
        if len(sys.argv) != 5:
            print("Usage: read <filename>")
            sys.exit(1)
        fname = sys.argv[4]

        try:
            result = client.manage_read(fname)

            if result is None or result.file_exists == False:
                print(f"Error: File '{fname}' not found in the system.")
                sys.exit(1)
            source_ip = result.host
            source_port = result.port
            if not os.path.exists(CLIENT_LOCAL_DIR):
                os.makedirs(CLIENT_LOCAL_DIR)

            source_client, source_transport = connect_to_replica(source_ip, source_port)
            if not source_client or not source_transport:
                print(f"Error: Unable to connect to the source replica at {source_ip}:{source_port}")
                sys.exit(1)

            local_file_path = os.path.join(CLIENT_LOCAL_DIR, fname)

            with open(local_file_path, "wb") as wf:
                offset = 0
                while True:
                    chunk = source_client.get_file_chunk(fname, offset, CHUNK_SIZE)
                    if not chunk:
                        break
                    wf.write(chunk)
                    offset += len(chunk)

            print(f"File '{fname}' downloaded to '{CLIENT_LOCAL_DIR}'")

        except Exception as e:
            print(f"Error reading file: {e}")
        finally:
            transport.close()

    elif operation == "write":
        if len(sys.argv) != 5:
                print("Usage: write <filepath>")
                sys.exit(1)
        filepath = sys.argv[4]

        try:
            fname = os.path.basename(filepath)
            with open(filepath, "rb") as f:
                data = f.read()
            client.manage_write(fname, data)
            print(f"Successfully wrote file '{fname}' to the system.")
        except Exception as e:
            print(f"Error writing file: {e}")
        finally:
            transport.close()

    elif operation == "list":
        try:
            file_versions = client.list_files()
            print("Files in the system:")
            for fname, version in file_versions.items():
                print(f"{fname} (version {version})")
        except Exception as e:
            print(f"Error listing files: {e}")
        finally:
            transport.close()

    else:
        print("Unknown operation. Use 'read', 'write', or 'list'.")
        sys.exit(1)

if __name__ == "__main__":
    main()
