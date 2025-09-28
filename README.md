

```markdown
# lsm_cache

Lightweight Log-Structured-Merge (LSM) style cache with optional replication. Implements an in-memory memtable backed by on-disk SSTables and a simple WAL; includes a replicated wrapper used by the demo `node1.py` & `node2.py`.

## Features
- Append-first, LSM-inspired memtable + SSTable persistence
- WAL for recovery
- Batch and single `put`, `read`, `delete` and range reads
- Simple leader-based replication via `ReplicatedLSMTree`
- Minimal, pure-Python implementation (no extra dependencies)

## Requirements
- Python 3.8+


## Quick start
- Start the demo server: run `server.py` (reads/writes use the LSM/replication code).
- Use `client.py` to send commands to the server.

Example usage (adapt to actual public API in the code):
    from server import LSMTree, ReplicatedLSMTree
    from client import CacheClient

    # standalone LSM
    lsm = LSMTree(max_length=1000, data_dir='data')
    lsm.put(1, 'value')
    print(lsm.read(1))

    # replicated nodes (demo)
    
    node = ReplicatedLSMTree(port=1234, all_ports= [ 1234 , 1235 ], leader=1234)
    from helpers import run_replica_server
    from server import ReplicatedLSMTree
    if __name__ == '__main__':
        port = 1234
        replicated_lsm_tree = ReplicatedLSMTree( 1234 , [ 1234 , 1235 ], 1234)
    run_replica_server(replicated_lsm_tree,port)




    from helpers import run_replica_server
    from server import ReplicatedLSMTree
    if __name__ == '__main__':
        port = 1235
        replicated_lsm_tree = ReplicatedLSMTree( 1235 , [ 1234 , 1235 ],1234 )
        run_replica_server(replicated_lsm_tree,port)

    # client example
    client = CacheClient(1235)
    client.put(1, 'hello')
    print(client.read(1))
    client = CacheClient(1234)
    print(client.read(1))


Tests in repository:
- `test_cache.py`
- `test_cache_persistent.py`
- `test_cache_replica.py`

## Project layout
- `server.py` — LSM and replication demo server
- `client.py` — simple TCP client helper
- WAL and SSTable files written to per-node data directories (e.g. `1234/`, `1235/`)
- `test/` and test files in repository root

```
