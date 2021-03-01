import re
import os
import time
import pickle
import gc
from tqdm import tqdm

# 日志聚类
class Logcluster:
	def __init__(self, logTemplate='', logIDL=None, eventId = None):
		self.logTemplate = logTemplate
		self.eventId = eventId
		if logIDL is None:
			logIDL = []
		self.logIDL = logIDL

# 构建树结构
class Node:
	def __init__(self, childD=None, depth=0, digitOrtoken=None):
		if childD is None:
			childD = dict()
		self.childD = childD
		self.depth = depth
		self.digitOrtoken = digitOrtoken

class Drain:

	"""
	树深度
	depth: depth of all leaf nodes
	正则表达式
	rex: regular expressions used in preprocessing (step1)
	相似阈值
	st: similarity threshold
	最大子节点数
	maxChild: max number of children of an internal node
	生成前缀树存储路径
	treeStoragePath: save path of prefix tree of Drain
	"""
	def __init__(self, depth=4, st=0.4, maxChild=100, rex = None,treeStoragePath=None):
		self.depth = depth
		self.st = st
		self.maxChild = maxChild
		if rex is None:
			rex = []
		self.rex = rex
		if treeStoragePath !=None:
			with open(treeStoragePath, 'rb') as treeStorage:
				self.prefixTree = pickle.load(treeStorage)
		else:
			self.prefixTree = Node()
		self.logClustes = []

	'''
	重新构建解析树
	'''
	def reconstruct(self):

		self.prefixTree = Node()
		self.logClustes = []

	'''
	根据输入日志构建解析树
	输入日志文件位置
	inputFile: the input path stores the input log file name
	输出日志标签位置
	outputFile: the output path stores the log labels
	'''
	def fit(self, inputFile ='D:\\毕业设计\\loghub\\HDFS_1\\HDFS.log', outputFile ='.\\log_item_to_label.csv', isReconstruct = False):

		if isReconstruct:
			self.reconstruct()
			f = open(outputFile, 'w')
		else:
			f = open(outputFile, 'a')

		t1 = time.time()

		# 为加快速度采用原生的文件写入方式
		f.write("LineId,BlockId,EventId\n")
		with open(inputFile) as lines:
			count = 0
			event_num = 1
			for line in tqdm(lines, desc="load data"):

				logmessageL = line.strip().split()
				logID = count
				# TODO 还是要通过传参方式来实现
				logmessageL = logmessageL[3:]
				cookedLine = ' '.join(logmessageL)

				blkId_list = re.findall(r'(blk_-?\d+)', cookedLine)
				for currentRex in self.rex:
					cookedLine = re.sub(currentRex, '', cookedLine)
				# cookedLine = re.sub(currentRex, 'core', cookedLine) #For BGL only
				# cookedLine = re.sub('node-[0-9]+','node-',cookedLine) #For HPC only

				logmessageL = cookedLine.split()
				matchCluster = self.treeSearch(logmessageL)
				# Match no existing log cluster
				if matchCluster is None:
					matchCluster = Logcluster(logTemplate=logmessageL, logIDL=[[logID, blkId_list]], eventId=event_num)
					event_num += 1
					self.logClustes.append(matchCluster)
					self.insert(matchCluster)

				# Add the new log message to the existing cluster
				else:
					# 用迭代的方法找到最佳的模板
					newTemplate = self.getTemplate(logmessageL, matchCluster.logTemplate)
					matchCluster.logIDL.append([logID, blkId_list])

					if ' '.join(newTemplate) != ' '.join(matchCluster.logTemplate):
						matchCluster.logTemplate = newTemplate
				f.write(str(logID) + "," + " ".join(blkId_list) + "," + str(matchCluster.eventId) + '\n')
				count += 1
				if count > 5000:
					break
		f.close()
		t2 = time.time()
		print('build the prefix tree process takes', t2 - t1)
		print('the number of log used to build the parser is ', count)
		print('*********************************************')
		gc.collect()
		self.printTree(self.prefixTree, self.depth)
		return t2 - t1

	'''
	用已生成解析树将指定位置日志解析并保存解析结果
	输入日志文件位置
	inputFile: the input path stores the input log file name
	输出日志标签位置
	outputFile: the output path stores the log labels
	'''
	def evaluate(self,logFile = 'D:\\毕业设计\\loghub\\HDFS_1\\HDFS.log', outputFile = '.\\log_item_to_label.csv'):

		t1 = time.time()

		f = open(outputFile, 'a')
		f.write("LineId,BlockId,EventId\n")
		with open(logFile) as lines:
			count = 0
			for line in tqdm(lines, desc="load data"):

				logmessageL = line.strip().split()
				logID = count
				logmessageL = logmessageL[3:]
				cookedLine = ' '.join(logmessageL)

				blkId_list = re.findall(r'(blk_-?\d+)', cookedLine)
				for currentRex in self.rex:
					cookedLine = re.sub(currentRex, '', cookedLine)
				# cookedLine = re.sub(currentRex, 'core', cookedLine) #For BGL only

				# cookedLine = re.sub('node-[0-9]+','node-',cookedLine) #For HPC only
				logmessageL = cookedLine.split()
				matchCluster = self.treeSearch(logmessageL)
				# Match no existing log cluster
				eventId = -1
				if matchCluster is not None:
					# 用迭代的方法找到最佳的模板
					eventId = matchCluster.eventId
				f.write(str(logID) + "," + " ".join(blkId_list) + "," + str(eventId) + '\n')
				count += 1
				if count > 5000:
					break
		f.close()
		t2 = time.time()
		print('evaluate process takes', t2 - t1)
		print('the number of log been parsed is ', count)
		print('*********************************************')
		gc.collect()
		self.printTree(self.prefixTree, self.depth)
		return t2 - t1

	'''
	保存已经生成好的解析树和日志类别
	'''
	def save(self,treeStorageFile = 'prefixTree.pkl', savePath='./results/', saveFileName='template', saveTempFileName='logTemplates.txt'):

		if not os.path.exists(savePath):
			os.makedirs(savePath)
		else:
			self.deleteAllFiles(savePath)
		f = open(savePath+treeStorageFile, 'wb')
		pickle.dump(self.prefixTree, f)

		logClusterSavePath = savePath+"logClusters/"
		if not os.path.exists(logClusterSavePath):
			os.makedirs(logClusterSavePath)
		else:
			self.deleteAllFiles(logClusterSavePath)

		writeTemplate = open(logClusterSavePath + saveTempFileName, 'w')
		idx = 1
		for logClust in self.logClustes:
			writeTemplate.write(' '.join(logClust.logTemplate) + '\n')
			writeID = open(logClusterSavePath + saveFileName + str(idx) + '.txt', 'w')
			for logID in logClust.logIDL:
				writeID.write(str(logID) + '\n')
			writeID.close()
			idx += 1

		writeTemplate.close()
		return None

	'''
	包含数字
	'''
	def hasNumbers(self, s):
		return any(char.isdigit() for char in s)

	'''
	在树上搜索
	'''
	def treeSearch(self, seq):
		# 返回聚类
		retLogClust = None

		# 序列长度
		seqLen = len(seq)
		parentn = self.prefixTree
		currentDepth = 0
		for token in seq:
			if currentDepth>=self.depth or currentDepth>seqLen:
				break

			if token in parentn.childD:
				parentn = parentn.childD[token]
			elif '*' in parentn.childD:
				parentn = parentn.childD['*']
			else:
				return retLogClust
			currentDepth += 1

		logClustL = parentn.childD

		retLogClust = self.FastMatch(logClustL, seq)

		return retLogClust


	'''
	往解析树上添加新类别的日志
	关键是维护树根
	'''
	def insert(self, logClust):
		seqLen = len(logClust.logTemplate)
		# if seqLen not in rn.childD:
		# 	firtLayerNode = Node(depth=1, digitOrtoken=seqLen)
		# 	rn.childD[seqLen] = firtLayerNode
		# else:
		# 	firtLayerNode = rn.childD[seqLen]

		parentn = self.prefixTree

		currentDepth = 0
		for token in logClust.logTemplate:

			#Add current log cluster to the leaf node
			#到尽头了
			if currentDepth>=self.depth or currentDepth>seqLen:
				if len(parentn.childD) == 0:
					parentn.childD = [logClust]
				else:
					parentn.childD.append(logClust)
				break

			#If token not matched in this layer of existing tree. 
			if token not in parentn.childD:
				if not self.hasNumbers(token):
					if '*' in parentn.childD:
						if len(parentn.childD) < self.maxChild:
							newNode = Node(depth=currentDepth+1, digitOrtoken=token)
							parentn.childD[token] = newNode
							parentn = newNode
						else:
							parentn = parentn.childD['*']
					else:
						if len(parentn.childD)+1 < self.maxChild:
							newNode = Node(depth=currentDepth+1, digitOrtoken=token)
							parentn.childD[token] = newNode
							parentn = newNode
						elif len(parentn.childD)+1 == self.maxChild:
							newNode = Node(depth=currentDepth+1, digitOrtoken='*')
							parentn.childD['*'] = newNode
							parentn = newNode
						else:
							parentn = parentn.childD['*']
			
				else:
					if '*' not in parentn.childD:
						newNode = Node(depth=currentDepth+1, digitOrtoken='*')
						parentn.childD['*'] = newNode
						parentn = newNode
					else:
						parentn = parentn.childD['*']

			#If the token is matched
			else:
				parentn = parentn.childD[token]

			currentDepth += 1

	#seq1 is template
	def SeqDist(self, seq1, seq2):
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

	'''
	快速匹配，直接基于相似度来匹配
	'''
	def FastMatch(self, logClustL, seq):
		retLogClust = None

		maxSim = -1
		maxNumOfPara = -1
		maxClust = None

		for logClust in logClustL:
			curSim, curNumOfPara = self.SeqDist(logClust.logTemplate, seq)
			if curSim>maxSim or (curSim==maxSim and curNumOfPara>maxNumOfPara):
				maxSim = curSim
				maxNumOfPara = curNumOfPara
				maxClust = logClust

		# 相似度已经大于阈值
		if maxSim >= self.st:
			retLogClust = maxClust	

		return retLogClust

	def deleteAllFiles(self, dirPath):
		fileList = os.listdir(dirPath)
		for fileName in fileList:
			path_name = os.path.join(dirPath,fileName)
			if os.path.isfile(path_name):
	 			os.remove(path_name)
			else:
				self.deleteAllFiles(path_name)

	'''
	拿到模板，可以用于InterLog的方法
	'''
	def getTemplate(self, seq1, seq2):
		# assert len(seq1) == len(seq2)
		retVal = []

		i = 0
		for word in seq1:
			if i>=len(seq2):
				break
			if word == seq2[i]:
				retVal.append(word)
			else:
				retVal.append('*')

			i += 1

		return retVal

	'''
	打印树
	'''
	def printTree(self, node, dep):
		pStr = ''	
		for i in range(dep):
			pStr += '\t'

		if node.depth == 0:
			pStr += 'Root Node'
		elif node.depth == 1:
			pStr += '<' + str(node.digitOrtoken) + '>'
		else:
			pStr += node.digitOrtoken

		print(pStr)

		if node.depth == self.depth:
			return 1
		for child in node.childD:
			self.printTree(node.childD[child], dep+1)



if __name__=="__main__":
	# HDFS parameters for example
	rex = ['blk_(|-)[0-9]+','(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)']
	myParser = Drain(treeStoragePath='./results/prefixTree.pkl',rex = rex)
	# myParser.fit()
	# myParser.save()
	myParser.evaluate(outputFile='./test_evalue.csv')
	print(" ")