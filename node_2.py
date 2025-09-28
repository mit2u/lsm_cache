from helpers import run_replica_server
from server import ReplicatedLSMTree

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    port = 1235
    replicated_lsm_tree = ReplicatedLSMTree( 1235 , [ 1234 , 1235 ],1234 )
    run_replica_server(replicated_lsm_tree,port)