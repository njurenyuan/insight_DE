import json
import sys
from datetime import datetime
from collections import deque

unix_origin = datetime(1970, 1, 1)

def extract_timestamp(tweet):
    ts = tweet['created_at']
    tt = ts[:19] + " " + ts[26:]
    return (datetime.strptime(tt, "%a %b %d %H:%M:%S %Y") - unix_origin).total_seconds()
    
def extract_hashtags(tweet):
    hashTag = []
    hashtags = tweet["entities"]["hashtags"]
    for group in hashtags:
        tag = group["text"]
        if len(tag) > 0:
            hashTag.append(tag)
    return hashTag

class edge_group():
    def __init__(self, timestamp):
        self.timestamp = timestamp
        self.edges = []

class twitter_graph():
    def __init__(self):
        self.edges = deque()
        self.nodes = {}
        self.timestamp_max = 0
        self.total_edges = 0
        self.total_nodes = 0
        
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
                return self.edges[int(timestamp - self.edges[0].timestamp)]
            
    def add_edge_to_group(self, edge_group, hash_tags):
        pointer = 0
        while pointer < len(hash_tags) - 1:
            p = hash_tags[pointer]
            for h in hash_tags[pointer+1:]:
                if p == h:
                    continue
                new_edge = [p, h] if p > h else [h, p]
                # if the edge already exists, remove it from old group
                for group in self.edges:
                    if new_edge in group.edges:
                        group.edges.remove(new_edge)
                        break
                else:
                    self.total_edges += 1
                    for n in new_edge:
                        if n in self.nodes:
                            self.nodes[n] += 1
                        else:
                            self.nodes[n] = 1
                            self.total_nodes += 1
                edge_group.edges.append(new_edge)
            pointer += 1
        
    def reomve_old_edge(self):
        try:
            while self.timestamp_max - self.edges[0].timestamp >= 60:
                old_edge_group = self.edges.popleft()
                self.total_edges -= len(old_edge_group.edges)
                for e in old_edge_group.edges:  # remove nodes in old edges
                    for n in e:
                        if self.nodes[n] > 1:
                            self.nodes[n] -= 1
                        else:
                            self.nodes.pop(n, None)
                            self.total_nodes -= 1
        except:
            pass
            
    def calculate_average_degree(self):
        if self.total_nodes > 0:
            return 2.0 * self.total_edges / self.total_nodes
        else:
            return 0.0
        
graph = twitter_graph()
fin = open(sys.argv[1], 'r')
fout = open(sys.argv[2], 'w')
while True:
    tweet = fin.readline()
    if len(tweet) == 0:
        break
    else:
        try:
            data = json.loads(tweet)
            timestamp = extract_timestamp(data)
            if timestamp > graph.timestamp_max:
                graph.timestamp_max = timestamp
            hash_tags = extract_hashtags(data)
            if len(hash_tags) > 1:
                new_edge_group = graph.find_group_for_new_edge(timestamp)
                graph.add_edge_to_group(new_edge_group, hash_tags)
            graph.reomve_old_edge()
            fout.write("%.2f\n" % graph.calculate_average_degree())
        except:
            pass
fin.close()
fout.close()