from server import ReplicatedLSMTree
import json
import threading
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    replicated_lsm_tree = ReplicatedLSMTree( 1234 , [ 1234 , 1235 ], 1234)
    server_socket = replicated_lsm_tree.socket
    print( f"Node  {1234} is starting as a leader." )
    #Start the background thread to check for leader timeouts
    threading.Thread( target = replicated_lsm_tree.check_election_timeout ).start()
    threading.Thread( target = replicated_lsm_tree.elect_leader ).start()
    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")
        # Now you can communicate with the client using client_socket
        # For example, receive data:
        data = client_socket.recv(1024)
        if not data:
            client_socket.close()  # Close the client socket after handling
        print(f"Received from client: {data}")
        data = data.decode()
        if data == 'alive':
            replicated_lsm_tree.internal_lsm.put_sstable([('last_heartbeat', time.time() )])
            client_socket.close()
            continue
        # Send a response:
        output = replicated_lsm_tree.decode_func_call( data )
        if output:
            client_socket.sendall(json.dumps(output).encode())
        client_socket.close() # Close the client socket after handling