import os



# seq1 is template
def SeqDist(seq1, seq2):
    # assert len(seq1) == len(seq2)
    simTokens = 0
    numOfPar = 0

    for token1, token2 in zip(seq1, seq2):
        if token1 == '*':
            numOfPar += 1
            continue
        if token1 == token2:
            simTokens += 1

    retVal = float(simTokens) / len(seq1)
    return retVal, numOfPar


def deleteAllFiles(dirPath):
    fileList = os.listdir(dirPath)
    for fileName in fileList:
        path_name = os.path.join(dirPath, fileName)
        if os.path.isfile(path_name):
            os.remove(path_name)
        else:
            deleteAllFiles(path_name)

'''
拿到模板，可以用于InterLog的方法
'''
def getTemplate(seq1, seq2):
    # assert len(seq1) == len(seq2)
    retVal = []

    i = 0
    for word in seq1:
        if i >= len(seq2):
            break
        if word == seq2[i]:
            retVal.append(word)
        else:
            retVal.append('*')

        i += 1

    return retVal


'''
包含数字
'''
def hasNumbers(s):
    # return any(char.isdigit() for char in s)
    return False

def getTemplate(seq1, seq2):
    # assert len(seq1) == len(seq2)
    retVal = []
    if not isinstance(seq1,list):
        seq1 = seq1.split()
    if not isinstance(seq2, list):
        seq2 = seq2.split()
    i = 0
    for word in seq1:
        if i>=len(seq2):
            break
        if word == seq2[i]:
            retVal.append(word)
        else:
            retVal.append('*')
        i += 1
    return ' '.join(retVal)

def compareSimilarity(node1,node2):
    childrenOfNode1 = node1.children.keys()
    childrenOfNode2 = node2.children.keys()
    print(childrenOfNode1,childrenOfNode2)
    retVal, numOfPar = SeqDist(list(childrenOfNode1),list(childrenOfNode2))
    # print(retVal)
    return retVal