To execute each of the components:

## Replica Server
python replica_server.py <local_directory> <compute_nodes_file> <port>

## Client 
To interact with the replica servers as a client: 
- python client.py <replica_ip> <replica_port> read <filename>
- python client.py <replica_ip> <replica_port> write <filepath>
- python client.py <replica_ip> <replica_port> list

---

Additionally, ensure the Thrift installation is within PA1's parent directory so:
- /parent
    - /parent/PA3
    - /parent/thrift-0.19.0
</br>
