import os
import shutil
import socket
import json
import time
class Node:
    def __init__(self, value,data,parent):
        self.value = value
        self.left = None  # Reference to the left child node
        self.right = None # Reference to the right child node
        self.parent = parent
        self.data = data

    def insert(self , key , value):
        current_node = self
        current_node_value = self.value
        if key < current_node_value :  # If the new data is smaller, go left
            if current_node.left is None:
                current_node.left = Node( key , value ,current_node)
            else:
                current_node.left.insert( key, value )  # Recursively call insert on the left child
        elif key > current_node_value :  # If the new data is larger, go right
            if current_node.right is None:
                current_node.right = Node( key , value ,current_node)
            else:
                current_node.right.insert( key, value )  # Recursively call insert on the right child

class BinaryTree:
    def __init__(self):
        self.root = None

    def insert(self , value, data):
        node = Node(value, data,None)
        if self.root is None:
            self.root = node
            return
        self.root.insert( value, data )

    def delete(self, value):
        node = self.search(value)
        if not node:
            return
        is_left_child = node.parent.left == node
        first_node = node.left
        if not first_node:
            first_node = node.right
            node.right = None
        parent = node.parent
        if first_node:
            first_node.parent = parent
        if is_left_child:
            parent.left = first_node
        else:
            parent.right = first_node
        if not first_node:
            self.root = seek_parent( parent )
            return True
        right_node = node.right
        if not right_node:
            self.root = seek_parent( self.parent )
            return True
        seek_node = first_node
        while True:
            right_child = seek_node.right
            if not right_child:
                break
            seek_node = right_child
        right_node.parent = seek_node
        seek_node.right = right_node
        self.root = seek_parent( parent )
        return True



    def search(self, value):
        current_node = self.root
        while current_node:
            if current_node.value == value:
                return current_node
            elif value < current_node.value:
                current_node = current_node.left
            else:
                current_node = current_node.right

def inorder_traversal( node,output_list,stop_node_key = None):
    if not node:
        return
    if stop_node_key and stop_node_key < node.value:
        return
    elif stop_node_key and stop_node_key == node.value:
        output_list.append( (node.value , node.data) )
        return
    inorder_traversal(node.left,output_list,stop_node_key)
    output_list.append((node.value,node.data))
    inorder_traversal(node.right,output_list,stop_node_key)

def inorder_insertion( input_list,parent_node=None):
    if not input_list:
        return
    input_list_length = len(input_list)
    median = input_list_length // 2
    value = input_list[median][1]
    key = input_list[median][0]
    node = Node(key,value,parent_node)
    if input_list_length == 1:
        return node
    node.left = inorder_insertion(input_list[:input_list_length//2],node)
    node.right = inorder_insertion(input_list[(input_list_length//2)+1:],node)
    return node

def seek_parent(node):
    if node.parent:
        return seek_parent(node.parent)

class KeyValueStore:
    def __init__(self):
        self.store = BinaryTree()

    def put(self, key, value):
        self.store.insert(key, value)

    def read(self, key):
        node = self.store.search(key)
        if not node:
            return None
        return node.data

    def read_key_range(self , start_key , end_key):
        keys_values = [ ]
        while start_key < end_key:
            current_node = self.store.search(start_key)
            if current_node:
                inorder_traversal(current_node, keys_values,stop_node_key=end_key)
                start_key = keys_values[-1][0] + 1
            else:
                start_key += 1
        return {k:v for k,v in keys_values}



    def delete(self, key):
        return self.store.delete(key)



class LSMTree(KeyValueStore):
    def __init__(self,max_length,data_dir = 'test'):
        super(LSMTree,self).__init__()
        try :
            shutil.rmtree( data_dir )
        except :
            pass
        os.makedirs( data_dir , exist_ok = True )
        self.max_length = max_length
        self.key_length = 0
        self.last_sstable_file = 0
        self.wal_log = data_dir+'wal_log.txt'
        self.data_dir = data_dir


    def _flush_memtable_to_sstable(self):
        output_list = []
        inorder_traversal( self.store.root , output_list)
        self.put_sstable( output_list )
        self.key_length = 0
        self.store = BinaryTree()


    def put(self, key, value):
        self._append_to_wal(  )
        if self.key_length >= self.max_length:
            self._flush_memtable_to_sstable()
        self.key_length += 1
        super(LSMTree,self).put(key, value)


    def read(self,key):
        found = super(LSMTree,self).read(key)
        if found:
            return found
        return self.sstable_search(key)

    def sstable_search(self,key):
        tables = self.last_sstable_file
        for table in range(tables,0,-1):
            value = self.sstable_get(key,table)
            if value is True:
                return
            if value:
                return value


    def sstable_get(self,key,table='1'):
        with open( self.data_dir + '/' + 'sstable' + str( table ) , 'r' ) as sstable :
            for line in sstable :
                line_split = line.split( ':' )
                try :
                    int_key = int( line_split[ 0 ] )
                except :
                    int_key = line_split[ 0 ]
                if int_key == key :
                    values = line_split[ 1 ].strip()
                    if values :
                        return values
                    return True

    def read_key_range(self , start_key , end_key):
        values_dict = super(LSMTree,self).read_key_range( start_key , end_key )
        tables = self.last_sstable_file
        keys_deleted = []
        for table in range(tables,0,-1):
            with open(self.data_dir+'/'+'sstable'+str(table), 'r') as sstable:
                for line in sstable:
                    line_split = line.split( ':' )
                    try:
                        key = int( line_split[ 0 ] )
                    except ValueError:
                        key = line_split[ 0 ]
                    if key > end_key:
                        break
                    elif key in keys_deleted :
                        continue
                    if key > start_key and key not in values_dict:
                        value = line_split[ 1 ].strip()
                        if not value:
                            keys_deleted.append(key)
                            continue
                        values_dict[ key ] = value
        return values_dict

    def batch_put(self, keys, values):
        self._append_to_wal()
        if self.key_length:
            self._flush_memtable_to_sstable()
        sorted_keys = sorted(list( zip(keys, values)), key=lambda x: x[0])
        len_keys = len( sorted_keys )
        if len_keys > self.max_length:
            self.put_sstable( sorted_keys )
            return
        last_item = None
        if not len_keys%2:
            last_item = sorted_keys.pop()
        self.store.root = inorder_insertion(sorted_keys)
        if last_item:
            self.store.put(last_item[0], last_item[1])

    def put_sstable (self , sorted_keys) :
        self.last_sstable_file += 1
        last_sstable = 'sstable' + str( self.last_sstable_file )
        self.sstable_put(  sorted_keys,last_sstable  )

    def sstable_put (self ,  sorted_keys,last_sstable ='sstable1') :
        with open( self.data_dir + '/' + last_sstable , 'w' ) as sstable :
            for key , value in sorted_keys :
                sstable.write( str( key ) + ':' + str( value ) + '\n' )

    def delete(self,key):
        self._append_to_wal()
        self.key_length -= 1
        super(LSMTree,self).delete(key)
        tables = self.last_sstable_file
        for table in range( tables, 0,-1) :
            found = False
            with open( self.data_dir + '/' + 'sstable'+str(table), 'r' ) as sstable :
                lines = sstable.readlines()
                for i, line in enumerate(lines) :
                    line_split = line.split( ':' )
                    if not line_split:
                        continue
                    try:
                        int_k = int( line_split[ 0 ] )
                    except :
                        int_k = line_split[ 0 ]
                    if int_k == key :
                        found = True
                        line = line.replace( line_split[ 1 ].strip() , '' )
                        lines[i] = line
                        break
            if found:
                with open( self.data_dir + '/' + 'sstable'+str(table) , 'w') as sstable:
                    sstable.writelines( lines )
                return

    def _append_to_wal(self):
        if not self.encoded_command:
            return
        with open(self.wal_log , 'a') as wal:
            wal.write( self.encoded_command +'\n')
        self.encoded_command = None

    def _restore_wal(self):
        self.root = None
        self.key_length = 0
        self.last_sstable_file = 0
        with open(self.wal_log , 'r') as wal:
            for line in wal.readlines():
                self.decode_func_call( line )

    def decode_func_call (self , line) :
        self.encoded_command = line
        func , args = line.split( '(' )
        args = args.replace( ')' , '' )
        args_list = [ ]
        for arg in args.split( ';' ) :
            arg = arg.strip()
            if '[' in arg :
                arg = eval( arg )
            elif arg.isdigit() :
                arg = int( arg )
            args_list.append( arg )
        return self.__getattribute__( func )( *args_list )

class ReplicatedLSMTree():
    lsm_instance = None
    port = None
    all_ports = None
    vote_count = 0
    election_timeout = 100  # seconds
    socket = None
    host = 'localhost'  # Or '0.0.0.0' to listen on all available interfaces
    internal_lsm = None

    def __init__(self, port,all_ports,leader):
        self.port = port
        self.all_ports = all_ports
        server_socket = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
        try :
            server_socket.bind( (self.host , port) )
        except :
            server_socket.close()
            server_socket = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
            server_socket.bind( (self.host , port) )
        server_socket.listen( 5 )  # Allow up to 5 queued connections
        print( f"Server listening on {self.host}:{port}" )
        self.socket= server_socket
        self.lsm_instance = LSMTree( 2048 ,str(self.port))
        self.internal_lsm = LSMTree( 2048 ,str(self.port*10 + 1))
        self.internal_lsm.sstable_put( [( 'leader' , leader ) ])
        try :
            self.lsm_instance._restore_wal()
        except :
            pass

    def broadcast_heartbeat(self) :
        leader = self.get_leader()
        if not leader == self.port:
            return
        # Leader broadcasts heartbeats to all followers
        for node in self.all_ports :
            if node != self.port:
                socket_conn = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
                socket_conn.connect( (self.host , node) )
                socket_conn.send('alive'.encode() )
                socket_conn.close()

    def check_election_timeout(self):
        while True:
            leader  = self.get_leader()
            print("Leader is", leader)
            if leader == self.port:
                self.broadcast_heartbeat()
                time.sleep( 50 )
                continue
            time.sleep(50)
            heatbeat = self.get_heartbeat()
            leader = self.get_leader()
            print("Heatbeat is", heatbeat)
            if leader == self.port and time.time() - int( heatbeat ) > self.election_timeout:
                print(f"Leader {leader} timed out. Starting election.")
                try:
                    self.start_election()
                except :
                    time.sleep( 0.5 )

    def get_heartbeat (self) :
        try:
            return int(float(self.internal_lsm.sstable_get( 'last_heartbeat' )))
        except Exception as e:
            print(e)
            return 0

    def start_election(self):
        for node in self.all_ports :
            if node != self.port:
                socket_conn = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
                socket_conn.connect( (self.host , node*10+1) )
                socket_conn.send(('leader:'+str(self.port)).encode())
                socket_conn.close()
        self.internal_lsm.sstable_put( [( 'leader' , self.port ) ])

    def elect_leader(self):
        while True :
            server_socket = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
            try :
                server_socket.bind( (self.host , self.port*10+1) )
            except :
                server_socket.close()
                server_socket = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
                server_socket.bind( (self.host , self.port*10+1) )
            server_socket.listen( 1 )  # Allow up to 5 queued connections
            print( f"Server listening on {self.host}:{self.port*10+1}" )
            client_socket , client_address = server_socket.accept()
            print( f"Accepted connection from {client_address}" )
            # Now you can communicate with the client using client_socket
            # For example, receive data:
            data = client_socket.recv( 1024 )
            if not data :
                client_socket.close()  # Close the client socket after handling
            print( f"Received from client: {data}" )
            # Send a response:
            data = data.decode()
            if data.startswith('leader'):
                self.internal_lsm.sstable_put( [( 'leader' ,int(data.split(':')[1]))] )
                self.internal_lsm.sstable_put ([('last_heartbeat' ,time.time())])
            client_socket.close()  # Close the client socket after handling

    def decode_func_call (self , line) :
        if line.startswith('batch_put(') or line.startswith('put(') or line.startswith('delete(') :
            try :
                self.replicate(line)
            except :
                pass
        return self.lsm_instance.decode_func_call ( line)

    def replicate (self,line) :
        encoded_command = line
        leader = self.get_leader()
        if leader == self.port :
            for node in self.all_ports :
                if node != self.port :
                    socket_conn = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
                    socket_conn.connect( (self.host , node) )
                    socket_conn.send( encoded_command.encode() )
                    socket_conn.close()

    def get_leader (self) :
        return int(self.internal_lsm.sstable_get( 'leader' ))




