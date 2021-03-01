from model import Node,Drain,buildSampleDrain
from optimizer import Optimizer
from plotter import createPlot
import os

if __name__ == "__main__":
    logPath = os.path.join(os.path.abspath('.'),'sample.log')
    drain = buildSampleDrain(logPath)
    t = drain.copy()
    # createPlot(t)
    opt = Optimizer()
    opt.modify(t)
    t = drain.copy(t)
    createPlot(t)
    # with open("./results/logClusters/logTemplates.txt") as lines:
    #     for line in lines:
    #         seq = line.split()
    #         t.addSeq(seq)
    # createPlot(t)