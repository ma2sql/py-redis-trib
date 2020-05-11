import time

def is_config_consistent(nodes):
    signatures=[]
    for n in nodes:
        signatures.append(n.get_config_signature())
    return len(set(signatures)) == 1


def wait_cluster_join(nodes):
    print("Waiting for the cluster to join")
    while not is_config_consistent(nodes):
        print(".", end="", flush=True)
        time.sleep(1)
    print()


