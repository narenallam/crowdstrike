import redis
from redisgraph import Node, Edge, Graph, Path
from data_generator import data_gen
from pprint import pprint

def dump_data(n_records):
    r = redis.Redis(host='localhost', port=6379)
    data = data_gen(n_records)
    redis_graph = Graph('file_activity1', r)
    nodes = {}
    edges = {}
    pprint(data)
    for rec in data:
        _node = Node(label='file', properties={'fid':rec['_id'], 'name':rec['name'], 'date_added':rec['date_added'], 'platform':rec['platform']})
        r.set(rec['_id'], _node.alias)
        redis_graph.add_node(_node)
        nodes[rec['_id']] = _node

    for rec in data:

        for fileid, time_stamp in rec['downloaded']:
            edge = Edge(nodes[rec['_id']] , 'DOWNLOADED', nodes[fileid], properties={'time': time_stamp, 'activity':'downloaded'})
            redis_graph.add_edge(edge)

        for fileid, time_stamp in rec['executed']:
            edge = Edge(nodes[rec['_id']] , 'EXECUTED', nodes[fileid], properties={'time': time_stamp, 'activity':'executed'})
            redis_graph.add_edge(edge)

        for fileid, time_stamp in rec['removed']:

            edge = Edge(nodes[rec['_id']] , 'REMOVED', nodes[fileid], properties={'time': time_stamp, 'activity':'removed'})
            redis_graph.add_edge(edge)

    redis_graph.commit()
    
    print("Graph created")
if __name__ == '__main__':
    dump_data(30)