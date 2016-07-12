import json
import sys
from datetime import datetime
from collections import deque

unix_origin = datetime(1970, 1, 1)

def extract_timestamp(trans):
    ts = trans['created_time']
    return (datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") - unix_origin).total_seconds()
    
class edge_group():
    def __init__(self, timestamp):
        self.timestamp = timestamp
        self.edges = []

class venmo_graph():
    def __init__(self):
        self.edges = deque()    # store groups of edges
        self.nodes = {}     # store number of degree for each node
        self.degrees = {}   # store histogram of degree
        self.timestamp_max = 0
        self.total_nodes = 0
        
    def increase_degree_in_histogram(self, degree):
        if degree in self.degrees:
            if self.degrees[degree] > 1:
                self.degrees[degree] -= 1
            else:
                self.degrees.pop(degree)
        new_degree = degree + 1
        if new_degree in self.degrees:
            self.degrees[new_degree] += 1
        else:
            self.degrees[new_degree] = 1
            
    def decrese_degree_in_histogram(self, degree):
        if self.degrees[degree] > 1:
            self.degrees[degree] -= 1
        else:
            self.degrees.pop(degree)
        new_degree = degree - 1
        if new_degree in self.degrees:
            self.degrees[new_degree] += 1
        elif new_degree > 0:
            self.degrees[new_degree] = 1
        
    def find_group_for_new_edge(self, timestamp):
        if len(self.edges) == 0:
            self.edges.append(edge_group(timestamp))
            return self.edges[-1]
        else:
            if timestamp > self.edges[-1].timestamp:
                time_max = self.edges[-1].timestamp
                n_new_group = int(timestamp - time_max) 
                for i in range(n_new_group):
                    self.edges.append(edge_group(time_max+i+1))
                return self.edges[-1]
            else:
                index = int(timestamp - self.edges[0].timestamp)
                if index >= 0:
                    return self.edges[index]
                else:
                    return None
            
    def add_edge_to_group(self, edge_group, actor, target):
        if target != actor:
            new_edge = [actor, target] if actor > target else [target, actor]
            # if the edge already exists, remove it from old group
            for group in self.edges:
                if new_edge in group.edges:
                    group.edges.remove(new_edge)
                    break
            else:
                for n in new_edge:
                    if n in self.nodes:
                        self.increase_degree_in_histogram(self.nodes[n])
                        self.nodes[n] += 1
                    else:
                        self.increase_degree_in_histogram(0)
                        self.nodes[n] = 1
                        self.total_nodes += 1
            edge_group.edges.append(new_edge)
        
    def reomve_old_edge(self):
        try:
            while self.timestamp_max - self.edges[0].timestamp >= 60:
                old_edge_group = self.edges.popleft()
                for e in old_edge_group.edges:  # remove nodes in old edges
                    for n in e:
                        if self.nodes[n] > 1:
                            self.decrese_degree_in_histogram(self.nodes[n])
                            self.nodes[n] -= 1
                        else:
                            self.decrese_degree_in_histogram(1)
                            self.nodes.pop(n, None)
                            self.total_nodes -= 1
        except:
            pass
            
    def calculate_median_degree(self):
        if self.total_nodes > 0:
            nodes = 0
            median_nodes = self.total_nodes / 2.0
            median_degree1 = None
            for d in sorted(self.degrees):
                nodes += self.degrees[d]
                if nodes == median_nodes:
                    median_degree1 = d
                elif nodes > median_nodes:
                    if median_degree1:
                        return (d + median_degree1) / 2.0
                    else:
                        return d
        else:
            return 0.0
        
graph = venmo_graph()
fin = open(sys.argv[1], 'r')
fout = open(sys.argv[2], 'w')
while True:
    trans = fin.readline()
    if len(trans) == 0:
        break
    else:
        try:
            data = json.loads(trans)
            timestamp = extract_timestamp(data)
            target = data['target']
            actor = data['actor']
            if timestamp > graph.timestamp_max:
                graph.timestamp_max = timestamp
            new_edge_group = graph.find_group_for_new_edge(timestamp)
            if new_edge_group:
                graph.add_edge_to_group(new_edge_group, actor, target)
                graph.reomve_old_edge()
            fout.write("%.2f\n" % graph.calculate_median_degree())
        except:
            pass
fin.close()
fout.close()