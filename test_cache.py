from client import CacheClient
if __name__ == '__main__':
    # Insert nodes
    CacheClient(1234).put( 10 , 'john' )
    CacheClient(1234).put( 5 , 'mitul' )
    CacheClient(1234).put( 15 , 'anmol' )
    CacheClient(1234).put( 2 , 'manas' )
    CacheClient(1234).put( 7 , 'shalini' )
    print( CacheClient(1234).read( 5 ) )
    print( CacheClient(1234).read( 7 ) )
    # Perform an inorder traversal
    print( "Range Keys" )
    print( CacheClient(1234).read_key_range( 1 , 15 ) )
    CacheClient(1234).batch_put( [ 1, 2, 3 ] , [ 'atul', 'muskan', 'swami' ] )
    print( CacheClient(1234).read_key_range( 1 , 15 ) )
    print("Deleted")
    print( CacheClient(1234).delete( 1 ) )
    print( CacheClient(1234).read_key_range( 1 , 15 ) )
    print( CacheClient(1234).read_key_range( 1 , 15 ) )