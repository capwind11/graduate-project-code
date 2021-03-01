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

    def partition_by_file(self):

        data_dict = OrderedDict()
        with open(self.para.log_item_to_label_file) as lines:
            for row in tqdm(lines,desc="map blockId to event sequence"):
                [_,blockId,eventId] = row.strip().split(',')
                if blockId=="BlockId":
                    continue
                # blockId = blockId_and_eventId[1]
                # blockId = blockId_and_eventId[2]
                blkId_list = blockId.split()
                blkId_set = set(blkId_list)
                for blk_Id in blkId_set:
                    if not blk_Id in data_dict:
                        data_dict[blk_Id] = []
                    data_dict[blk_Id].append(int(eventId))
        data_df = pd.DataFrame(list(data_dict.items()), columns=['BlockId', 'EventSequence'])
        data_df.to_csv(self.para.instances_file_path)

    def map_log_seq_to_label(self,data_df):

        blockId_to_label = pd.read_csv(self.para.anormal_file_path, engine='c', na_filter=False, memory_map=True)
        blockId_to_label = blockId_to_label.set_index('BlockId')
        label_dict = blockId_to_label['Label'].to_dict()
        data_df['Label'] = data_df['BlockId'].apply(lambda x: 1 if label_dict[x] == 'Anomaly' else 0)
        # normaly_output
        data_df.to_csv('data_instances.csv')


if __name__=='__main__':
    para = Para('./dev_data/','D:/毕业设计/loghub/HDFS_1/',"log_item_to_label.csv",'data_instances_hdfs.csv','anomaly_label.csv','dev_normaly.log','dev_abnormaly.log')
    preprocess = Preprocess(para)
    # blockId_to_logs = preprocess.partition_by_file()
    data_df = pd.read_csv("./dev_data/data_instances_hdfs.csv", index_col=0,engine='c', na_filter=False, memory_map=True)
    preprocess.map_log_seq_to_label(data_df)
