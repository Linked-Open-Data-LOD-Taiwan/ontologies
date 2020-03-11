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
import xml.etree.ElementTree as etree

#extend
import pandas as pd 
import pandasql as ps
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import requests
import json

#library
import lib.globalclasses as gc
from lib.const import *

wiki_query_str= """
            SELECT DISTINCT ?river ?riverLabel ?destLabel ?river_length ?riverAltLabel WHERE {
          ?river wdt:P17 wd:Q865;
            wdt:P31 wd:Q4022.
            OPTIONAL {?river wdt:P403 ?dest.}
            OPTIONAL {?river wdt:P2043 ?river_length.}
          
          SERVICE wikibase:label { bd:serviceParam wikibase:language "zh". }
        }
        ORDER BY DESC(?length)
            """
##### Code section #####
def xml_to_df(filename):
    
    tree = etree.parse(filename)
    root = tree.getroot()
    
    dataname = root[0].tag #FIME: need more check no data
    colnames = []
    for name in root[0]:
        colnames.append(name.tag)
    df = pd.DataFrame(columns = colnames)
    for member in root.findall(dataname):
        cols = []
        for i in range(len(colnames)):
            result = member.find(colnames[i])
            cols.append(result.text if result is not None else None)
        df = df.append(pd.Series(cols, index = colnames), ignore_index = True)
    return df


#temple_df = xml_to_df("dataset/TEDS/8203_temple.xml",["寺廟名稱", "主祀神祇", "行政區","地址","教別","建別","組織型態","電話","負責人","其他","WGS84X","WGS84Y"])
#temple_df.to_csv('output/temple.csv')

#church_df = xml_to_df("dataset/church.xml")

#church_df = xml_to_df("dataset/church.xml",'OpenData_4',["教會名稱", "行政區", "地址","負責人","WGS84X","WGS84Y"])
#church_df.to_csv('output/church.csv')

#Spec: manage opendata
#How/NeedToKnow:
class DataMgr():
    def __init__(self):
        self.d_df = pd.read_csv("include/dbs.csv")
        
        self.d_df['wkg_obj'] = None
        self.d_df['wkg_df'] = None

        for index, row in self.d_df.iterrows():
            #print(row['資料集識別碼'], row['資料集名稱'], row['主要欄位說明'])
            list_url = [row['url'],row['local_file']]
            print(list_url)
            name = row['name']
            if name=="go":
                mgr = OpenDataMgr(list_url)
                self.odMgr = mgr
            elif name=="ld":
                list_url = [None,row['local_file']]
                mgr = LocalDataMgr(list_url)
                self.ldMgr = mgr
            else:
                mgr = DataMgr(list_url)
            self.d_df['wkg_obj'][name] = mgr
            self.d_df['wkg_df'][name] = mgr.df
        self.d_df.set_index('name', inplace=True)
        

#Spec: manage opendata
#How/NeedToKnow:
class DataMgrBase():
    def __init__(self,list_url):
        if list_url[0] is None:
            self.df = pd.read_csv("include/" + list_url[1])
        else:
            self.url_to_file(list_url[0], 'output/' +list_url[1] )
            self.df = pd.read_csv("output/" + list_url[1] )
    def url_to_file(self,url,pathname,reload=False):
        print("url_to_file: url=%s, pathname=%s" %(url,pathname))
        need_load= True
        if reload==False and os.path.isfile(pathname) :
            need_load = False
        if need_load:
            r = requests.get(url, allow_redirects=True,verify=False)
            open(pathname, 'wb').write(r.content)

#Spec: manage opendata
#How/NeedToKnow:
class LocalDataMgr(DataMgrBase):
    def __init__(self,list_url):
        DataMgrBase.__init__(self,list_url)
        self.ld_df = self.df
        self.ld_df['wkg_df'] = None
        self.load_localdf(False)
        self.ld_df.set_index('name', inplace=True)
    def load_df(self,dfname):
        row =self.ld_df.loc[dfname]
        url = row['url']
        dir = "include/"
        if not pd.isnull(url):
            dir = "output/"
            self.url_to_file(row['url'], dir +row['local_file'] )
        df = pd.read_csv(dir + row['local_file'] )
        self.ld_df['wkg_df'][dfname]=df
        return df
    def load_localdf(self, need_load=False):
        df_list = []
        for index, row in self.ld_df.iterrows():
            #print("index=%s,local_file=%s" %(index,row['local_file']))
            if pd.isnull(row['url']):
                #print("%s" % (row['local_file']))
                df = pd.read_csv("include/" + row['local_file'] )
                df_list.append(df)
            else:
                if need_load:
                    self.url_to_file(row['url'], 'output/' +row['local_file'] )
                    df = pd.read_csv("output/" + row['local_file'] )
                    df_list.append(df)
                else:
                    df_list.append(None)
        self.ld_df['wkg_df'] = df_list
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
#Spec: manage opendata
#How/NeedToKnow:

class OpenDataMgr(DataMgrBase):
    def __init__(self,list_url):
        #private
        
        #global: these variables allow to direct access from outside.

        DataMgrBase.__init__(self,list_url) 
        self.od_df = self.df
        self.od_df.set_index('資料集識別碼', inplace=True)
        self.od_df.sort_values(by=['資料集識別碼'], inplace=True,ascending=True)
        self.od_df['df'] = None # init new column to store dataframe
        #print(self.od_df.head())
        self.river_df = None 
        self.rivercode_df = None
        self.colname_df = None
        self.term_df = pd.read_csv("include/term.csv") 
        
    

    def get_datasets(self,dataset_ids,force=False): # dataset_ids: [ int, ... ]
        dfs = []
        for dataset_id in dataset_ids:
            dfs.append(self.get_dataset(dataset_id, force))
        return dfs
    # CSV > XML
    def get_dataset(self,dataset_id,force=False): #integer
        if not dataset_id in self.od_df.index:
            print("%i 資料集不存在" %(dataset_id))
            return None
        type_str = self.od_df['檔案格式'][dataset_id]
        name = self.od_df['資料集名稱'][dataset_id]
        types = type_str.split(";")
        pos = 0
        pos_csv = -1
        pos_xml = -1
        for typename in types:
            if typename == "CSV":
                pos_csv = pos               
            if typename == "XML":
                pos_xml = pos
            pos +=1
        print("pos_csv=%i,pos_xml=%i" %(pos_csv,pos_xml))   
        if pos_csv >=0 or pos_xml>=0 :
            download_str = self.od_df['資料下載網址'][dataset_id]
            urls = download_str.split(";")
            if pos_csv >=0:
                filename = "output/%s-%s.csv" %( dataset_id, name)
                self.url_to_file(urls[pos_csv],filename,force)
                df = pd.read_csv(filename)
                
            else: #XML
                filename = "output/%s-%s.xml" %( dataset_id, name)
                self.url_to_file(urls[pos_xml],filename,force)
                df = xml_to_df(filename)
                #df = pd.read_csv("output/%s.csv" %( dataset_id ))
            self.od_df['df'][dataset_id]=df
                
            return df
        else:
            print("%i 資料集沒有 CSV,XML 格式，無法載入" %(dataset_id))
            return None
    def get_riverlist(self):
        did = 22228
        title=[['BasinIdentifier','BasinName','EnglishBasinName'],
            ['SubsidiaryBasinIdentifier','SubsidiaryBasinName','EnglishSubsidiaryBasinName'],
            ['SubSubsidiaryBasinIdentifier','SubSubsidiaryBasinName','EnglishSubSubsidiaryBasinName'],
            ['SubSubSubsidiaryBasinIdentifier','SubSubSubsidiaryBasinName','EnglishSubSubSubsidiaryBasinName']]
        #print(title)
    
        self.get_dataset(did,False)
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
    def get_colmap(self,od_df=None):

        #wra=self.od_df[self.od_df['提供機關']=='經濟部水利署']
        #desc=wra[['資料集名稱','主要欄位說明']]
        if od_df is None:
            od_df = self.od_df
        desc=od_df[['資料集名稱','主要欄位說明']]
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
            colname_groupby_df = self.colname_df.groupby('Colname')['資料集名稱'].count().sort_values(ascending=False)
            freq_df = colname_groupby_df[colname_groupby_df>=count_min]
    
            # 需要忽略的詞
            ignore_df=self.term_df['欄位忽略詞'].dropna().reset_index([0],'欄位忽略詞')
            
            # 剩要處理的詞        
            mon_list = list(set(freq_df.index) - set(ignore_df.values))
            mon_s = pd.Series(mon_list) 
        
        # 要處理的 colname 資訊
        dot_df=self.colname_df[self.colname_df['Colname'].isin(mon_s.values)]
        
        # 格式化輸出        
        myprint = lambda row: "\"%s\" -> \"%s\";" %(row[1],row[0]) 
        print("digraph G {\n")       
        print(dot_df.apply(myprint, axis=1).to_string(index=False))
        print("}\n")
    def test(self):

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


class WikiDataMgr():
    def __init__(self):
        pass

        
    def wikidata_get(self,filename,query):
        url = 'https://query.wikidata.org/sparql'
        if not os.path.isfile('output/' + filename):
            r = requests.get(url, params = {'format': 'json', 'query': query})
            #r = requests.get(url, allow_redirects=True)
            open('output/' + filename, 'wb').write(r.content)
        #df = pd.read_csv("output/wikidata.csv")
    def load_json(self,filename):
        with open('output/' + filename, 'r') as json_file:
            data = json.load(json_file)
        #data = json.loads('output/' + filename)
        #processed_results = json.load(result.response)
        cols = data['head']['vars']
    
        out = []
        for row in data['results']['bindings']:
            item = []
            for c in cols:
                item.append(row.get(c, {}).get('value'))
            out.append(item)
    
        return pd.DataFrame(out, columns=cols)
        #return pd.read_json('output/' + filename)
# Interactive mode
# from codes.opendata import *
if sys.argv[0] == '':
    #interactive
    dMgr = DataMgr()
    d_df= dMgr.d_df
    odMgr = dMgr.odMgr
    ldMgr = dMgr.ldMgr
    od_df = dMgr.odMgr.od_df
    ld_df = dMgr.ldMgr.ld_df

    river_df = odMgr.get_riverlist()
    rivercode_df = odMgr.rivercode_df
    colname_df = odMgr.get_colmap()
    term_df = ld_df['wkg_df']['term']
    print("DataMgr inited for interactive used")
    print("dMgr,odMgr,ldMgr,od_df,ld_df,rivercode_df,river_df,colname_df,term_df variable ready")
    #odMgr.gen_col_tree(False,5)