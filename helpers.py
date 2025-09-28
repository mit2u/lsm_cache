import json
import threading
import time


def run_replica_server (replicated_lsm_tree,port) :
    global data
    server_socket = replicated_lsm_tree.socket
    print( f"Node  {port} is starting as a follower." )
    # Start the background thread to check for leader timeouts
    threading.Thread( target = replicated_lsm_tree.check_election_timeout ).start()
    threading.Thread( target = replicated_lsm_tree.elect_leader ).start()
    while True :
        client_socket , client_address = server_socket.accept()
        print( f"Accepted connection from {client_address}" )
        # Now you can communicate with the client using client_socket
        # For example, receive data:
        data = client_socket.recv( 1024 )
        if not data :
            client_socket.close()  # Close the client socket after handling
        print( f"Received from client: {data}" )
        data = data.decode()
        if data == 'alive' :
            replicated_lsm_tree.internal_lsm.sstable_put( [ ('last_heartbeat' , time.time()) ] )
            client_socket.close()
            continue
        output = replicated_lsm_tree.decode_func_call( data )
        if output :
            client_socket.sendall( json.dumps( output ).encode() )
        client_socket.close()  # Close the client socket after handling
