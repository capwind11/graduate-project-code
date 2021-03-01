# -*- coding:utf-8 -*-
import pandas as pd
import re
from collections import OrderedDict
from tqdm import tqdm

class Para:
    def __init__(self,outputFileDir,inputFileDir,log_item_to_label_file,instances_file_path,anormal_file_path,normaly_output,abnormaly_output):
        self.outputFileDir = outputFileDir
        self.inputFileDir = inputFileDir
        self.log_item_to_label_file = outputFileDir+log_item_to_label_file
        self.instances_file_path = outputFileDir+instances_file_path
        self.anormal_file_path =inputFileDir+anormal_file_path
        self.normaly_output = outputFileDir+normaly_output
        self.abnormaly_output = outputFileDir+abnormaly_output

class Preprocess:

    def __init__(self,para):
        self.para = para

    def partition(self):
        struct_log = pd.read_csv(self.para.log_item_to_label_file, engine='c', na_filter=False, memory_map=True)
        data_dict = OrderedDict()
        for idx, row in tqdm(struct_log.iterrows(),desc="map blockId to event sequence"):
            blkId_list = row['BlockId'].split()
            blkId_set = set(blkId_list)
            for blk_Id in blkId_set:
                if not blk_Id in data_dict:
                    data_dict[blk_Id] = []
                data_dict[blk_Id].append(row['EventId'])
        data_df = pd.DataFrame(list(data_dict.items()), columns=['BlockId', 'EventSequence'])
        data_df.to_csv(self.para.instances_file_path)

    def save_data_instance(self,blockId_to_logs):
        file_name = self.para.instances_file_path
        with open(file_name, "w") as f:
            f.write("BlockId,EventIds \n")
            for key in blockId_to_logs.keys():
                f.write(key + "," + " ".join(list(map(str, blockId_to_logs[key]))) + "\n")

    def map_log_seq_to_label(self,data_dict):
        data_df = pd.DataFrame(list(data_dict.items()), columns=['BlockId', 'EventSequence'])
        # Split training and validation set in a class-uniform way
        #     label_data = pd.read_csv(label_file, engine='c', na_filter=False, memory_map=True)
        blockId_to_label = pd.read_csv(self.para.anormal_file_path, engine='c', na_filter=False, memory_map=True)
        label_dict = blockId_to_label['Label'].to_dict()
        data_df['Label'] = data_df['BlockId'].apply(lambda x: 1 if label_dict[x] == 'Anomaly' else 0)
        # normaly_output
        data_df.to_csv('data_instances.csv', index=False)

    def extract_log_seq_of_diff_label(self,blockId_to_logs):
        normal_seq = []
        abnormal_seq = []
        blockId_to_label = pd.read_csv(self.para.anormal_file_path, engine='c', na_filter=False, memory_map=True)
        log_instances = pd.read_csv(self.para.instances_file_path, engine='c', na_filter=False, memory_map=True)
        max_len = min(len(blockId_to_label),len(log_instances))
        for i in range(max_len):
            if blockId_to_label.iloc[i]['BlockId']!=log_instances.iloc[i]['BlockId']:
                continue
            if blockId_to_label.iloc[i]['Label'] == 'Normal':
                normal_seq.append(log_instances.iloc[i][1])
            else:
                abnormal_seq.append(log_instances.iloc[i][1])
        return [normal_seq,abnormal_seq]

    def save_diff_label_data(self,normal_seq,abnormal_seq):
        normaly_output = self.para.normaly_output
        abnormaly_output = self.para.abnormaly_output
        with open(normaly_output, 'w') as f:
            for i in range(len(normal_seq)):
                f.write( normal_seq[i] + '\n')
        with open(abnormaly_output,'w') as f:
            for i in range(len(abnormal_seq)):
                f.write(abnormal_seq[i]+'\n')

if __name__=='__main__':
    para = Para('./dev_data/','D:/毕业设计/loghub/HDFS_1/',"log_item_to_label.csv",'data_instances_hdfs.csv','anomaly_label.csv','dev_normaly.log','dev_abnormaly.log')
    preprocess = Preprocess(para)
    blockId_to_logs = preprocess.partition()
    # [normal_seq,abnormal_seq] = preprocess.extract_log_seq_of_diff_label(blockId_to_logs)
    # preprocess.save_diff_label_data(normal_seq,abnormal_seq)
