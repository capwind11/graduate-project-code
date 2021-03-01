from utils import *
from model import Node,Drain,buildSampleDrain
# 方法1： 合并子树节点

class Optimizer:

    '''
    用递归的方式合并子树
    '''
    def merge(self,nodeList):
        print([node.token for node in nodeList])
        res = nodeList[0]
        if len(nodeList) == 1 or len(res.children) == 0:
            return res
        if isinstance(res.children,list):
            for node in nodeList:
                res.children.extend(node.children)
            return res
        nodeDict = {}
        for node in nodeList:
            # if :
            for token in node.children.keys():
                child = node.children[token]
                if child.token not in nodeDict:
                    nodeDict[child.token] = []
                nodeDict[child.token].append(child)
        print(nodeDict.keys())
        for token in nodeDict.keys():
            res.children[token] = self.merge(nodeDict[token])
        return res

    def combineLeaves(self, nodeList):
        res = []
        ans = Node()
        template = nodeList[0].logTemplate
        for node in nodeList[1:]:
            template = getTemplate(template, node.logTemplate)
        ans.token = template
        res.append([ans])
        return res

    '''
    合并相似的节点
    '''
    def groupNodes(self,nodeList):
        res = []
        isWordGrouped = {node.token: False for node in nodeList}
        # print(isWordGrouped)
        if isinstance(nodeList[0].children,list):
            for node in nodeList:
                res.append([node])
            return res
        for i in range(len(nodeList)):
            node = nodeList[i]
            if isWordGrouped[node.token]:
                continue
            newGroup = [node]
            isWordGrouped[node.token] = True
            for node1 in nodeList[i + 1:]:
                newMember = []
                for member in newGroup:
                    print(member.token, node1.token)
                    if compareSimilarity(member, node1) > 0.75:
                        newMember.append(node1)
                        isWordGrouped[node1.token] = True
                newGroup.extend(newMember)
            res.append(newGroup)
        return res

    def modify(self,node):
        childs = node.children
        if len(childs) == 0 or isinstance(childs,list):
            return node
        nodeGroups = self.groupNodes(list(childs.values()))
        print(nodeGroups)
        node.children.clear()
        for group in nodeGroups:
            if len(group) > 1:
                node.children['*: ' + ';'.join([nod.token for nod in group])] = self.merge(group)
            else:
                node.children[group[0].token] = group[0]
        # createPlot(node)
        #     return node
        # try:
        for child in node.children.keys():
            node.children[child] = self.modify(node.children[child])
        # except:
        #     print(node.token)
        #     print(node.children)
        #     return node
        print(node.children)
        return node
