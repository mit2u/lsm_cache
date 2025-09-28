import socket
import json
class CacheClient:
    socket = None
    host = 'localhost'

    def __init__(self,port):
        self.socket = socket.socket( socket.AF_INET , socket.SOCK_STREAM )
        self.socket.connect( ('localhost' , port) )

    def send_command(self, command):
        self.socket.send( str(command).encode() )
        data = self.socket.recv(4096).decode()
        if data:
            return json.loads(data)

    def put(self,key,value):
        return self.send_command(f'put({key};{value})')

    def delete (self,key) :
        return self.send_command(f'delete({key})')

    def read(self,key):
        return self.send_command(f'read({key})')

    def read_key_range(self,start_key , end_key):
        return self.send_command(f'read_key_range({start_key};{end_key})')


    def batch_put(self,keys,values):
        self.send_command(f'batch_put({keys}; {values})')