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

    Res manage_read(1:string fname)
    i32 manage_write(1:string fname, 2:binary data)
    i32 get_versionnum(1:string fname)
    void set_versionnum(1:string fname, 2:i32 versionnum)
    void replicate(1:string fname, 2:binary data, 3:i32 version)
    map<string, i32> list_files()
    map<string, i32> get_local_files()
}
