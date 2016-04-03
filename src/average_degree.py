import json
import os
import time
from datetime import datetime
from collections import deque

if os.path.exists(r"../tweet_input/tweets.txt"):
    fin = open(r"../tweet_input/tweets.txt", 'r')
else:
    print "Error: tweets.txt not found!"
if not os.path.exists(r"../tweet_output"):
    os.makedirs(r"../tweet_output")
fout = open(r"../tweet_output/output.txt", 'w')
edges = deque()
nodes = {}
timestamp_max = 0
unix_origin = datetime(1970, 1, 1)
total_edges = 0
total_nodes = 0 
while True:
    tweet = fin.readline()
    if len(tweet) == 0:
        break
    else:
        try:
            data = json.loads(tweet)
            # extract timestamp
            ts = data['created_at']
            tt = ts[:19] + " " + ts[26:]
            timestamp = (datetime.strptime(tt, "%a %b %d %H:%M:%S %Y") - unix_origin).total_seconds()
            if timestamp > timestamp_max:
                timestamp_max = timestamp
            # extract hashtag
            hashTag = []
            hashtags = data["entities"]["hashtags"]
            for group in hashtags:
                tag = group["text"]
                if len(tag) > 0:
                    hashTag.append(tag)
            # add hashtag in edges
            length = len(hashTag)
            if length > 1:
                # find out which group new edges need to be added
                if len(edges) == 0:
                    edges.append([timestamp, []])
                    new_edge_group = edges[-1][1]
                else:
                    if timestamp > edges[-1][0]:
                        time_max = edges[-1][0]
                        n_new_group = int(timestamp - time_max) 
                        for i in range(n_new_group):
                            edges.append([time_max+i+1, []])
                        new_edge_group = edges[-1][1]
                    else:
                        new_edge_group = edges[int(timestamp - edges[0][0])][1]
                # add edges into the group
                pointer = 0
                while pointer < length - 1:
                    p = hashTag[pointer]
                    for h in hashTag[pointer+1:]:
                        if p == h:
                            continue
                        new_edge = [p, h] if p > h else [h, p]
                        # if the edge already exists, remove it from old group
                        for ts, edge_group in edges:
                            if new_edge in edge_group:
                                edge_group.remove(new_edge)
                                break
                        else:
                            total_edges += 1
                            for n in new_edge:
                                if n in nodes:
                                    nodes[n] += 1
                                else:
                                    nodes[n] = 1
                                    total_nodes += 1
                        new_edge_group.append(new_edge)
                    pointer += 1
            # remove old edges
            try:
                while timestamp_max - edges[0][0] >= 60:
                    ts, old_edges = edges.popleft()
                    total_edges -= len(old_edges)
                    for e in old_edges:  # remove nodes in old edges
                        for n in e:
                            if nodes[n] > 1:
                                nodes[n] -= 1
                            else:
                                nodes.pop(n, None)
                                total_nodes -= 1
            except:
                pass
            # calculate average degree
            if total_nodes > 0:
                fout.write("%.2f\n" % (2.0 * total_edges / total_nodes))
            else:
                fout.write("0.00\n")
        except:
            pass

fin.close()
fout.close()