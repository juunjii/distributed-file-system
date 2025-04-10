import os
from thrift import Thrift
from thrift.server import TServer
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from dfs import ReplicaService

CHUNK_SIZE = 2048

class ReplicaHandler:
    def __init__(self, directory):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def getFileSize(self, filename):
        path = os.path.join(self.directory, filename)
        return os.path.getsize(path)

  
    def writeFileChunk(self, filename, data, offset):
        path = os.path.join(self.directory, filename)
        with open(path, "r+b" if os.path.exists(path) else "wb") as f:
            f.seek(offset)
            f.write(data)

    def getFileChunk(self, filename, offset, chunk_size):
        path = os.path.join(self.directory, filename)
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(chunk_size)


    def requestFileCopy(self, filename, source_ip, source_port):
        from dfs import ReplicaService
        from thrift.transport import TSocket, TTransport
        from thrift.protocol import TBinaryProtocol

        # Connect to source replica
        transport = TSocket.TSocket(source_ip, source_port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ReplicaService.Client(protocol)
        transport.open()

        size = client.getFileSize(filename)
        with open(os.path.join(self.directory, filename), "wb") as f:
            offset = 0
            while offset < size:
                chunk = client.getFileChunk(filename, offset, CHUNK_SIZE)
                f.write(chunk)
                offset += len(chunk)

        transport.close()

def main():
    import sys
    if len(sys.argv) != 3:
        print("Usage: python replica.py [port] [directory]")
        return

    port = int(sys.argv[1])
    directory = sys.argv[2]

    handler = ReplicaHandler(directory)
    processor = ReplicaService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print(f"Replica running on port {port}, storing files in {directory}")
    server.serve()

if __name__ == "__main__":
    main()
