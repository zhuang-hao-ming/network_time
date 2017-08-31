import networkx as nx
import time
from query import get_edges, get_nodes


def build_graph():
    nodes = get_nodes()
    g = nx.Graph()
    
    for node in nodes:
        id = node[0]
        g.add_node(id)    
    
    edges = get_edges()

    for edge in edges:
        source, target, cost = edge
        g.add_edge(source, target, cost=cost)
        if True:
            g.add_edge(target, source, cost=cost)
    
    # print nx.dijkstra_path_length(g,123,445, weight='cost')
    return g

ROAD_NETWORK_GRAPH = build_graph()

def get_path_length(source, target):
    start_time = time.time()
    len = nx.dijkstra_path_length(ROAD_NETWORK_GRAPH, source,target, weight='cost')
    #print len
    print('elapse {0}'.format(time.time() - start_time))
    return len
if __name__ == '__main__':
    
    print get_path_length(100, 100)
    
