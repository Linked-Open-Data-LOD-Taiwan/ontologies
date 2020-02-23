# @file opendata.py
# @brief Manager for opendata access and process
# README:
# MODULE_ARCH:  
# CLASS_ARCH:
# GLOBAL USAGE: 
#    support interactive mode
#standard
import requests
import os.path
import sys
from collections import OrderedDict
#extend
import pandas as pd 
import numpy as np

#library
import lib.globalclasses as gc
from lib.const import *

##### Code section #####
#Spec: manage opendata
#How/NeedToKnow:
"""
from codes.opendata import *
odMgr = OpenDataMgr()
did = 22228

odMgr.get_dataset(did)
odMgr.od_df['df'][did].head()

#odMgr.od_df.keys()
#odMgr.od_df.loc[did]
#odMgr.od_df.at[did,'資料集名稱']

"""
class OpenDataMgr():
    def __init__(self):
        #private
        opendata_list_url = ["https://data.gov.tw/datasets/export/csv?type=dataset&order=pubdate&qs=&uid=",'opendata_list.csv']
        
        #global: these variables allow to direct access from outside.
        self.url_to_file(opendata_list_url[0], 'output/' +opendata_list_url[1] )

        self.od_df = pd.read_csv("output/" + opendata_list_url[1] ) 
        self.od_df.set_index('資料集識別碼', inplace=True)
        self.od_df.sort_values(by=['資料集識別碼'], inplace=True,ascending=True)
        self.od_df['df'] = None # init new column to store dataframe
        #print(self.od_df.head())
        self.river_df = None 
        self.rivercode_df = None
        self.colname_df = None
        
    
    def url_to_file(self,url,pathname,reload=False):

        need_load= True
        if reload==False and os.path.isfile(pathname) :
            need_load = False
        if need_load:
            r = requests.get(url, allow_redirects=True,verify=False)
            open(pathname, 'wb').write(r.content)


    def get_dataset(self,dataset_id): #integer
        if not dataset_id in self.od_df.index:
            return None
        type_str = self.od_df['檔案格式'][dataset_id]
        types = type_str.split(";")
        pos = 0
        for type in types:
            if type == "CSV":
                break
            pos +=1
        if pos < len(types):
            download_str = self.od_df['資料下載網址'][dataset_id]
            urls = download_str.split(";")
            
            self.url_to_file(urls[pos],"output/%s.csv" %( dataset_id))
            df = pd.read_csv("output/%s.csv" %( dataset_id ))
            self.od_df['df'][dataset_id]=df
            return df
        else:
            return None
    def get_riverlist(self):
        did = 22228
        title=[['BasinIdentifier','BasinName','EnglishBasinName'],
            ['SubsidiaryBasinIdentifier','SubsidiaryBasinName','EnglishSubsidiaryBasinName'],
            ['SubSubsidiaryBasinIdentifier','SubSubsidiaryBasinName','EnglishSubSubsidiaryBasinName'],
            ['SubSubSubsidiaryBasinIdentifier','SubSubSubsidiaryBasinName','EnglishSubSubSubsidiaryBasinName']]
        #print(title)
    
        self.get_dataset(did)
        self.rivercode_df = self.od_df['df'][did]
        
        rivers = [] # Id,[name, EName, ToId]
        rivers_id = []
        for index, item in self.rivercode_df.iterrows():
            
            
            GId=item['GovernmentUnitIdentifier']
            preId="0" # ocean
            for i in range(4):
                
                if not pd.isnull(item[title[i][0]]):
                #if not Id==0:
                    Id=item[title[i][0]]    
                    Name=item[title[i][1]]   
                    EName=item[title[i][2]] 
                    ToId=preId
                    
                    if not Id in rivers_id:
                        od = OrderedDict({
                            'Id':Id,
                            'Name':Name,
                            'EName':EName,
                            'ToId':ToId,
                            'GId':GId
                            })
                        #print(od)
                        rivers_id.append(Id)
                        rivers.append(od)
                        #rivers[Id] = [Name,EName,ToId,GId]
                    preId = Id
        self.river_df = pd.DataFrame(rivers)
        self.river_df.set_index('Id', inplace=True)
        #print(self.river_df)
        return self.river_df
    def get_colmap(self):

        wra=self.od_df[self.od_df['提供機關']=='經濟部水利署']
        desc=wra[['資料集名稱','主要欄位說明']]
        desc=desc.dropna()
        
        self.colname_df = pd.DataFrame(desc['主要欄位說明'].str.split(';').tolist(), index=desc['資料集名稱']).stack() 
        self.colname_df = self.colname_df.reset_index([0, '資料集名稱'])
        self.colname_df.columns = ['資料集名稱', 'Colname'] 
        return self.colname_df           
    def desc(self, desc_id):
        pass
# Interactive mode
# from codes.opendata import *
if sys.argv[0] == '':
    #interactive
    odMgr = OpenDataMgr()
    od_df = odMgr.od_df
    river_df = odMgr.get_riverlist()
    rivercode_df = odMgr.rivercode_df
    colname_df = odMgr.get_colmap()
    print("OpenDataMgr inited for interactive used")
    print("odMgr,od_df,rivercode_df,river_df,colname_df variable ready")