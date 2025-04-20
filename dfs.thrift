namespace py dfs

struct Res {
    1: i32 version,
    2: string host,
    3: i32 port,
    4: bool file_exists
}

service ReplicaService {

    // Fetch size of file
    i64 get_file_size(1: string fname)

    // Get a chunk of file data
    binary get_file_chunk(1: string fname, 2: i64 offset, 3: i64 chunk_size)

    // Write a chunk to local file
    void writeFileChunk(1: string fname, 2: binary data, 3: i64 offset)

    // Copy file from another replica
    void request_file(1: string fname, 2: string ip, 3: i32 port)

    // accepts read requests from clients
    Res manage_read(1:string fname)

    // accepts writes from clients
    i32 manage_write(1:string fname, 2:binary data)

    // gets version num of a certain file
    i32 get_versionnum(1:string fname)

    // sets file version num
    void set_versionnum(1:string fname, 2:i32 versionnum)

    // replicates a file locally from another machine
    void replicate(1:string fname, 2:binary data, 3:i32 version)

    // lists all files in system
    map<string, i32> list_files()

    // provides local files to caller
    map<string, i32> get_local_files()
}
