To execute each of the components:

## Replica Server
python3 replica_server.py <local_directory> <compute_nodes_file> &lt;port&gt;

## Client 
To interact with the replica servers as a client: 
- python3 client.py <replica_ip> <replica_port> read &lt;filename&gt;
- python3 client.py <replica_ip> <replica_port> write &lt;filepath&gt;
- python3 client.py <replica_ip> <replica_port> list

---

Additionally, ensure the Thrift installation is within PA1's parent directory so:
- /parent
    - /parent/PA3
    - /parent/thrift-0.19.0
</br>
