from model import Node,Drain,buildSampleDrain
# from optimizer import Optimizer
from optimizer import Optimizer
from plotter import createPlot
import os

def printClusters(logClusters):
    for logCluster in logClusters:
        print("eventId: "+str(logCluster.eventId)+" template: "+' '.join(logCluster.logTemplate),end=' ')
        print("parentNode: " ,end=' ')
        for node in logCluster.parentNode:
            print(node.token,end=' ')
        print()

def optimize_by_seq_dist():
    logPath = os.path.join(os.path.abspath(''), 'sample.log')
    drain = buildSampleDrain(logPath)
    logClusters = drain.logClusters
    printClusters(logClusters)
    opt = Optimizer()
    opt.modify(method = 'seq_dist',tree = drain.prefixTree,drain=drain,st = 0.7)
    logClusters = drain.logClusters
    printClusters(logClusters)
    root = drain.copy()
    createPlot(root)

def optimize_by_merge_sub_tree():
    logPath = os.path.join(os.path.abspath(''), 'sample.log')
    drain = buildSampleDrain(logPath)
    t = drain.copy()
    createPlot(t)
    t = drain.copy()
    opt = Optimizer()
    opt.modify(method='merge_sub_tree', tree=t)
    # opt.modify(t)
    createPlot(t)

def optimize_by_tfidf():
    logPath = os.path.join(os.path.abspath(''), 'sample.log')
    drain = buildSampleDrain(logPath)
    logClusters = drain.logClusters
    printClusters(logClusters)
    opt = Optimizer()
    opt.modify(method = 'tfidf',tree = drain.prefixTree,drain=drain,st = 0.6)
    logClusters = drain.logClusters
    print( )
    printClusters(logClusters)
    root = drain.copy()
    createPlot(root)


if __name__ == "__main__":
    # optimize_by_tfidf()
    # optimize_by_seq_dist()
    optimize_by_merge_sub_tree()