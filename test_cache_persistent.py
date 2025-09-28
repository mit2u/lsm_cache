from client import CacheClient
if __name__ == '__main__':
    # Insert nodes
    print( CacheClient(1234).read_key_range( 1 , 15 ) )
    print( CacheClient(1234).read( 1 ) )