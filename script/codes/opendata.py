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
        self.term_df = pd.read_csv("include/term.csv") 
        
    
    def url_to_file(self,url,pathname,reload=False):

        need_load= True
        if reload==False and os.path.isfile(pathname) :
            need_load = False
        if need_load:
            r = requests.get(url, allow_redirects=True,verify=False)
            open(pathname, 'wb').write(r.content)


    def get_dataset(self,dataset_id): #integer
        if not dataset_id in self.od_df.index:
            print("%i 資料集不存在" %(dataset_id))
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
            print("%i 資料集沒有 CSV 格式，無法載入" %(dataset_id))
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
    # generate dot , col->資料集名稱
    # while == True: 只產生白名單的
    # 其他 count >= count_min, not in term.欄位忽略詞 
    
    def gen_col_tree(self, white, count_min=5):
        if white:
            # 需要的詞
            mon_s=self.term_df['觀察欄位詞'].dropna().reset_index([0],'觀察欄位詞')

        else:
            #欄位 count > 5
            colname_groupby_df = colname_df.groupby('Colname')['資料集名稱'].count().sort_values(ascending=False)
            freq_df = colname_groupby_df[colname_groupby_df>=count_min]
    
            # 需要忽略的詞
            ignore_df=self.term_df['欄位忽略詞'].dropna().reset_index([0],'欄位忽略詞')
            
            # 剩要處理的詞        
            mon_list = list(set(freq_df.index) - set(ignore_df.values))
            mon_s = pd.Series(mon_list) 
        
        # 要處理的 colname 資訊
        dot_df=colname_df[colname_df['Colname'].isin(mon_s.values)]
        
        # 格式化輸出        
        myprint = lambda row: "\"%s\" -> \"%s\";" %(row[1],row[0]) 
        print("digraph G {\n")       
        print(dot_df.apply(myprint, axis=1).to_string(index=False))
        print("}\n")
    def test(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        import numpy as np
        df=self.get_dataset(6504)
        myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')
        df.plot('地區','牙醫數')
        #df.plot()
        #df.plot(x='地區',y='牙醫數')
        df.plot(x='地區',y='病床數',kind='bar') #line type have problem
        plt.title("測試",fontproperties=myfont) 
        plt.ylabel('地區',fontproperties=myfont)
        plt.xlabel('病床數',fontproperties=myfont)
        #plt.yticks(fontproperties=myfont)
        #plt.xticks(np.arange(0, 1, step=0.2))
        #plt.xticks(size='small',rotation=30,fontproperties=myfont)
        plt.legend(prop=myfont)
        plt.xticks(fontname = 'SimSun',size=8)
        plt.yticks(fontname = 'SimSun',size=8)
        #plt.show()
        plt.show()       
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
    term_df = odMgr.term_df
    print("OpenDataMgr inited for interactive used")
    print("odMgr,od_df,rivercode_df,river_df,colname_df,term_df variable ready")
    #odMgr.gen_col_tree(False,5)