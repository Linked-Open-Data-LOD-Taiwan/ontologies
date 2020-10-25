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
import os
import zipfile
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
            self.url_to_file(list_url[0], 'output/' +list_url[1],False )
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
    # download user define link to file
    def user_download(self,set_filename):
        """
            可提供一個檔案，指定要下載的相關資訊與指引，然後下載全部
        """
        self.user_download_df = pd.read_csv(set_filename)
        #op    file_type    local_file    encoding    projection    url
        for index, row in self.user_download_df.iterrows():
            if row['op']!='SKIP':
                self.url_to_file(row['url'], 'output/' +row['local_file'] )
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
        self.download_op_df = None # config, status information while download
        self.download_op = "" # transfer current download op, not interference to current code
        self.status_list = ["","","","",""]  #status_list=[real_filetype,localfile,status,last_url,sys_memo]
            #collect this dataset information status
        
        self.term_df = pd.read_csv("include/term.csv")     

    def get_datasets_by_downloadop(self):
        """
            follow op from download_op_cfg.csv, collection information found while downloading.
            use self.download_op_df,self.download_op,self.status_list
        """
        self.download_op_df = pd.read_csv("include/download_op_cfg.csv")
        #dataset_id    dataset_name    op    group    user_memo    encoding    projection    real_filetype    localfile    status    last_url    sys_memo
        for index, row in self.download_op_df.iterrows():
            if row['op']!='SKIP':
                self.download_op = row['op']
                self.get_dataset(row['dataset_id'],False)
                self.update_downloadop_statusinfo(row['dataset_id'], self.status_list)
    
    def update_downloadop_statusinfo(self,dataset_id,status_list):
        """
            #status_list=[real_filetype,localfile,status,last_url,sys_memo]
        """
        
        if self.download_op_df is not None:
             mask = (self.download_op_df['dataset_id']==dataset_id)
             self.download_op_df.loc[mask,['real_filetype','localfile','status','last_url','sys_memo']]=status_list
    # update one column of one dataset
    def update_downloadop(self,dataset_id,col_name, value, b_append=False):
        if self.download_op_df is not None:
            mask = (self.download_op_df['dataset_id']==dataset_id)
            if b_append:
                #self.download_op_df.loc[self.download_op_df['dataset_id']==dataset_id,col_name]=self.download_op_df[col_name] + "\n" + value
                self.download_op_df.loc[mask,col_name]="%s\n%s" %(self.download_op_df.loc[mask,col_name] ,value)
            else:
                self.download_op_df.loc[mask,col_name]=value
    #save result to filiie
    def output_downloadop(self):
        if self.download_op_df is not None:
            self.download_op_df.to_csv("output/download_op.csv")
            print("output/download_op.csv outputed!")
    
    def unzip_zips_indir(self,search_dir):
        
        
        """
            在指定目錄中，找下面的 zip 檔，解開到一暫存目錄，然後將裡面唯一的目錄，改名成跟 zip 檔名一樣
            目前用在 水利署 shp 檔，所以原目錄名稱原則上為 英文名稱，需要被保留。所以附在新目錄名稱中
            執行完畢，tmp 目錄會遺留下來，也可以手動刪除
        """
        #setup
        extract_dir =  "%s/tmp" %(search_dir)
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)
        target_dir="%s/zip" %(search_dir)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
    
        for dirpath, dirnames, filenames in os.walk(search_dir):
            #print("dirpath=%s\ndirnames=%s\nfilenames=%s" %(dirpath, dirnames, filenames))
            for filename in [f for f in filenames if f.endswith(".zip")]: #"25776-水庫堰壩位置圖.zip"
                zip_file = os.path.join(dirpath, filename)   
                
                #extract zip
                print("extracting %s" %(zip_file))
                with zipfile.ZipFile(zip_file,"r") as zip_ref:
                    zip_ref.extractall(extract_dir) 
                dirs = os.listdir(extract_dir)
                basename = os.path.basename(zip_file)    
                base = os. path. splitext(basename)[0] 
                
                if os.path.isdir(dirs[0]):
                    old_path = "%s/%s" %(extract_dir,dirs[0])
                    new_path = "%s/%s-%s" %(target_dir,base,dirs[0]) 
                    if os.path.isdir(old_path): 
                        os.rename(old_path,new_path)
                    #else:
                    #    print("%s not normal format, dirs=%s" %(zip_file,str(dirs)))
                else:
                    #move all to new dir    
                    ename = os. path. splitext(dirs[0])[0]
                    new_path = "%s/%s-%s" %(target_dir,base, ename) 
                    if not os.path.exists(new_path):
                        os.makedirs(new_path)
                    for file in dirs:
                        old_path = "%s/%s" %(extract_dir,file)
                        new_file = "%s/%s" %(new_path,file)
                        os.rename(old_path,new_file)
                print("done %s" %(zip_file))
                #time.sleep(3)
    def get_datasets(self,dataset_ids,force=False): # dataset_ids: [ int, ... ]
        dfs = []
        for dataset_id in dataset_ids:
            dfs.append(self.get_dataset(dataset_id, force))
        return dfs
    # CSV > XML
    def download_info_fromdf(self,ser,file_type): #return [typename,encode,url]
        type_str = str(ser['檔案格式'])
        types = type_str.split(";")
        pos = 0
        for typename in types:
            if typename == file_type:
                break               
            pos +=1
        if pos<len(types):
            encode_str = str(ser['編碼格式'])
            encodes = encode_str.split(";")
            download_str = str(ser['資料下載網址'])
            urls = download_str.split(";")
            return [typename,encodes[pos],urls[pos]]
        return None
            
        
    def get_dataset(self,dataset_id,force=False): #integer
        """
            找 dataset 的資料，有 CSV 先下載，然後是 XML, 然後是 KML
            如果 CSV 內提供 KML, SHP 連結，正常也會解析後下載
            下載過程會準備填回 download_op_df, 可以當作下載的輔助資料
        """
        real_filetype=""
        #localfile="" #filename
        filename =""
        status = "OK"
        url=""
        sys_memo_str=""
        #self.status_list = [real_filetype,filename,status,url,sys_memo_str]
        if not dataset_id in self.od_df.index:
            print("%i 資料集不存在" %(dataset_id))
            return None
        type_str = self.od_df['檔案格式'][dataset_id]
        encode_str = self.od_df['編碼格式'][dataset_id] 
        name = self.od_df['資料集名稱'][dataset_id]
        types = type_str.split(";")
        encodes = encode_str.split(";")
        pos = 0
        pos_csv = -1
        pos_xml = -1
        pos_kml = -1
        for typename in types:
            if typename == "CSV":
                pos_csv = pos               
            if typename == "XML":
                pos_xml = pos
            if typename == "KML":
                pos_kml = pos
            pos +=1
        print("pos_csv=%i,pos_xml=%i,pos_kml=%i" %(pos_csv,pos_xml,pos_kml))   
        if pos_csv >=0 or pos_xml>=0 or pos_kml>=0:
            download_str = self.od_df['資料下載網址'][dataset_id]
            urls = download_str.split(";")
            if pos_csv >=0:
                real_filetype="CSV"
                filename = "output/%s-%s.csv" %( dataset_id, name)
                #self.update_downloadop(dataset_id,'real_filetype','CSV')
                url = urls[pos_csv]
                self.url_to_file(url,filename,force)
                
                try:
                    if os.stat(filename).st_size == 0:
                        
                        self.status_list = [real_filetype,filename,"KO",url,"ERROR: file size = 0 "]
                        #self.update_downloadop(dataset_id,'sys_memo',,False)
                        return None
                    df = pd.read_csv(filename,encoding = encodes[pos_csv])
                    sys_memo_str=""
                    if len(df.index)<=10 or self.download_op=='DIG':
                        sys_memo_str ="WARNING: %s only have %i rows" %(filename,len(df.index)) 
                        print(sys_memo_str)
                        
                        #檔案描述    檔案格式    資源網址
                        #formats= df[df['檔案格式']=='SHP'].values.tolist()
                        if '檔案格式' in df.columns:
                            for index, row in df.iterrows():
                                format_str= row['檔案格式'].lower()
                                if format_str=='kml' or format_str=='shp' or format_str=='7z': 
                                    real_filetype = format_str
                                    if format_str=='shp':
                                        real_filetype = 'zip'
                                        if index==0:
                                            filename = "output/%s-%s.%s" %( dataset_id, name, real_filetype)
                                        else:
                                            filename = "output/%s-%s-%i.%s" %( dataset_id, name, index, real_filetype)
                                        sys_memo_str1="Get additional SHP in ZIP:%s" %(filename) 
                                    elif format_str=='kml':
                                        if index==0:
                                            filename = "output/%s-%s.%s" %( dataset_id, name, real_filetype)
                                        else:
                                            filename = "output/%s-%s-%i.%s" %( dataset_id, name, index, real_filetype)
                                        sys_memo_str1="Get additional %s:%s" %(format_str.upper(),filename) 
                                    else: #7z
                                        if index==0:
                                            filename = "output/%s-%s.%s" %( dataset_id, name, real_filetype)
                                        else:
                                            filename = "output/%s-%s-%i.%s" %( dataset_id, name, index, real_filetype)
                                        sys_memo_str1="Get additional %s:%s" %(format_str.upper(),filename) 
                                    url = row['資源網址']    
                                    self.url_to_file(url,filename,force)
                                    #self.update_downloadop(dataset_id,'last_url',url,False)
                                    print(sys_memo_str1)
                                    sys_memo_str = "%s\n%s" %(sys_memo_str,sys_memo_str1)
                                    #self.update_downloadop(dataset_id,'real_filetype',real_filetype.upper())
                        #self.update_downloadop(dataset_id,'sys_memo',sys_memo_str,False)
                    self.status_list = [real_filetype.upper(),filename,status,url,sys_memo_str]

                except:
                    sys_memo_str1 = "ERROR: %s have exception" %(filename)
                    print(sys_memo_str1)
                    self.status_list = [real_filetype,filename,"KO",url,sys_memo_str1]
                    return None
            elif pos_xml>=0: #XML
                real_filetype="XML"
                filename = "output/%s-%s.xml" %( dataset_id, name)
                url = urls[pos_xml]
                self.url_to_file(url,filename,force)
                #self.update_downloadop(dataset_id,'last_url',url,False)
                #self.update_downloadop(dataset_id,'real_filetype','XML')
                df = xml_to_df(filename)
                self.status_list = [real_filetype,filename,"OK",url,""]
                #df = pd.read_csv("output/%s.csv" %( dataset_id ))
            else: #KML
                real_filetype="KML"
                filename = "output/%s-%s.kml" %( dataset_id, name)
                url = urls[pos_kml]
                self.url_to_file(url,filename,force)
                #self.update_downloadop(dataset_id,'real_filetype','KML')
                #self.update_downloadop(dataset_id,'last_url',url,False)
                print("%i 資料集為 KML 格式，已下載，無法載入成 df. 支援格式： %s" %(dataset_id,type_str))
                self.status_list = [real_filetype,filename,"OK",url,""]
                return None
            self.od_df['df'][dataset_id]=df
                
            return df
        else:
            print("%i-%s 資料集沒有 CSV,XML,KML 格式，無法載入. 支援格式： %s" %(dataset_id,name,type_str))
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

        df=od_df
        df['ID'] = df.index
        desc=df[['ID','主要欄位說明']]
        desc=desc.dropna()
        df2 = pd.DataFrame(desc['主要欄位說明'].str.split(';').tolist(), index=desc['ID']).stack()
        df2 = df2.reset_index([0, 'ID'])
        df2.columns = ['DSID', 'Colname'] 
        
        df3 = pd.merge(df2, self.od_df, left_on='DSID',right_on='資料集識別碼', how='left',left_index=True)
        df3 = df3[['DSID','資料集名稱','Colname']]
        self.colname_df = df3.reset_index(drop=True)
        
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