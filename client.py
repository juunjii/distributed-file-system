
import os
from thrift import Thrift
from thrift.server import TServer
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

# copy_test.py
from dfs import ReplicaService
from replica_server import ReplicaHandler

# Trigger the file copy from replica2 (replica running on port 9091)
handler = ReplicaHandler("./replica2_data")
success = handler.request_file("test.txt", "127.0.0.1", 9090)

if success != -1:
    print("File copy triggered successfully.")