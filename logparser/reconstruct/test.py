from drain import drain_utils

if __name__=='__main__':
    seq1 = "e dsf sdf"
    seq2 = "e1 dsf sdf"
    print(drain_utils.SeqDist(seq1.split(),seq2.split()))

