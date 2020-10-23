
def cluster_nodes():
    return [
        {'node_id': '54b3cd517c7ce508630b9c9366cd4da19681fee7', 'addr': '192.168.56.101:6789', 'flags': 'master', 'slots': [['0', '5460']], 'migrating': {1: '2bd45a5a7ec0b5cb316d2e9073bb84c7ba81eea3'}},
        {'node_id': '2bd45a5a7ec0b5cb316d2e9073bb84c7ba81eea3', 'addr': '192.168.56.102:6789', 'flags': 'master', 'slots': [['5461', '10922']], 'importing': {1: '54b3cd517c7ce508630b9c9366cd4da19681fee7'}},
        {'node_id': '91d5f362ba127c8e0aba925f8e005f8b08054042', 'addr': '192.168.56.103:6789', 'flags': 'master', 'slots': [['10923','16383']]},
    ]
