# @file simple.py
# @brief the sample file that copied for new python file
# README:
# MODULE_ARCH:  
# CLASS_ARCH:
# GLOBAL USAGE: 
#standard
#extend
#library
import lib.globalclasses as gc
from lib.const import *

##### Code section #####
def wikidata_get():
    import requests
    
    url = 'https://query.wikidata.org/sparql'
    query = """
    SELECT DISTINCT ?river ?riverLabel ?destLabel ?river_length ?riverAltLabel WHERE {
  ?river wdt:P17 wd:Q865;
    wdt:P31 wd:Q4022.
    OPTIONAL {?river wdt:P403 ?dest.}
    OPTIONAL {?river wdt:P2043 ?river_length.}
  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "zh". }
}
ORDER BY DESC(?length)
    """
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
    #print(data)

    import pandas as pd
    from collections import OrderedDict
    
    rivers = []
    for item in data['results']['bindings']:
        #print(item)
        rivers.append(OrderedDict({
            'river': item['river']['value'],
            'riverLabel': item['riverLabel']['value'],
            'destLabel': item['destLabel']['value']
                if 'destLabel' in item else None,
            'river_length': item['river_length']['value']
                if 'river_length' in item else None,
            'riverAltLabel': item['riverAltLabel']['value']
                if 'riverAltLabel' in item else None
                }))
    
    
    df = pd.DataFrame(rivers)
    df.set_index('river', inplace=True)
    df = df.astype({'river_length': float})
    df.sort_values(by=['river_length'], inplace=True,ascending=False)
    #print(df)
    return df
    
    
    #%matplotlib notebook
    import matplotlib.pyplot as plt
    plt.style.use('ggplot')  
    plt.figure(figsize=(16, 12))

    label = 'river_length'
    df_plot = df[label].sort_values().dropna()    
    df_plot.plot(kind='barh', color='C0', ax=plt.gca());
    plt.ylabel('')
    plt.xticks(rotation=30)
    plt.title(label.capitalize())
    plt.yticks(fontname = 'SimSun',size=8)
    plt.show()

#河川代碼: https://data.gov.tw/dataset/22228
#https://data.wra.gov.tw/Service/OpenData.aspx?format=json&id=336F84F7-7CFF-4084-9698-813DD1A916FE
#~/Downloads/336F84F7-7CFF-4084-9698-813DD1A916FE.json
# 一條河的資訊： Id    Name    EName GId ToId
#BasinIdentifier    
#BasinName    
#EnglishBasinName    
#EnglishSubsidiaryBasinName    
#EnglishSubSubsidiaryBasinName    
#EnglishSubSubSubsidiaryBasinName    
#GovernmentUnitIdentifier    
#Remarks    
#SubsidiaryBasinIdentifier    
#SubsidiaryBasinName    
#SubSubsidiaryBasinIdentifier    
#SubSubsidiaryBasinName    
#SubSubSubsidiaryBasinIdentifier    
#SubSubSubsidiaryBasinName 

def opendata_getbynet(b_print = False):
    import requests
    url = 'https://data.wra.gov.tw/Service/OpenData.aspx'
    r = requests.get(url, verify=False, params = {'format': 'json', 'id': '336F84F7-7CFF-4084-9698-813DD1A916FE'})
    r.encoding = "utf-8-sig"
    data = r.json()
    if b_print:
        print(data)

#河川代碼: https://data.gov.tw/dataset/22228     
#return {['Id','Name','EName','ToId','GId'],[]} 
def opendata_get(b_print = False):
    title=[['BasinIdentifier','BasinName','EnglishBasinName'],
        ['SubsidiaryBasinIdentifier','SubsidiaryBasinName','EnglishSubsidiaryBasinName'],
        ['SubSubsidiaryBasinIdentifier','SubSubsidiaryBasinName','EnglishSubSubsidiaryBasinName'],
        ['SubSubSubsidiaryBasinIdentifier','SubSubSubsidiaryBasinName','EnglishSubSubSubsidiaryBasinName']]
    #print(title)

    import json
    with open('include/336F84F7-7CFF-4084-9698-813DD1A916FE.json' , 'r',encoding='utf-8-sig') as json_file:
        data = json.load(json_file)
    #print(data)

    rivers = {} # Id,[name, EName, ToId]
    for item in data['RiverCode_OPENDATA']:
        #print(item)
        #print(item['SubsidiaryBasinName'])
        GId=item['GovernmentUnitIdentifier'].strip() 
        preId="0" # ocean
        for i in range(4):
            if not item[title[i][0]]=="":
                Id=item[title[i][0]].strip()     
                Name=item[title[i][1]].strip()    
                EName=item[title[i][2]].strip() 
                ToId=preId
                
                if not Id in rivers:
                    rivers[Id] = [Name,EName,ToId,GId]
                preId = Id
    if b_print:            
        print("%10s,%20s,%20s,%10s,%10s" % ('Id','Name','EName','ToId','GId'))
        for id in sorted(rivers.keys()):
            print("%10s,%20s,%20s,%10s,%10s" %(id,rivers[id][0],rivers[id][1],rivers[id][2],rivers[id][3]))    
        print("river count=%i" %(len(rivers)))
    return rivers

# river to dot
"""
digraph G {

  a1 -> b3;
  b2 -> a3;
  a3 -> a0;
}
"""
def river_tree():
    rivers = opendata_get()
    dot_str="digraph G {\n"
    for key in sorted(rivers.keys()):
        toid = rivers[key][2]
        if toid =="0":
            gname = "0_海"
        else:
            gname = "%s_%s" % (toid,rivers[toid][0])
        dot_str += "\t\"%s_%s\" -> \"%s\";\n" %(key,rivers[key][0],gname )
    dot_str += "}"

    print(dot_str)
# river info compare from opendata diff with wikidata
def river_comapre():
    
    opendata = opendata_get()
    
    opendata_rivername = [opendata[x][0] for x in opendata]
    #print(opendata_rivername)
    
     
    wikidata = wikidata_get()
    #print(list(wikidata['riverLabel']))
     
    wikidata_rivername = list(wikidata['riverLabel'])
    wikidata_riveraltlabel = [x for x in list(wikidata['riverAltLabel']) if x is not None]
    #wikidata_riveraltname = list(wikidata['riverAltLabel'])
    wikidata_riveraltname = []
    for x in wikidata_riveraltlabel:
        cols = x.split(",")
        for col in cols:
            wikidata_riveraltname.append(col.strip())
        #wikidata_riveraltname.extend(cols)
    #map(str.strip, wikidata_riveraltname)
    print("--- wikidata_riveraltname, count = %i---" %(len(wikidata_riveraltname)))
    print( "\n".join(wikidata_riveraltname))
       
    #in opendata_rivername but not in wikidata_rivername
    diff_set = set(opendata_rivername).difference(set(wikidata_rivername))
    print("---in opendata_rivername but not in wikidata_rivername, count = %i---" %(len(diff_set)))
    print( "\n".join(diff_set))
    
    #opendata_rivername - wikidata_rivername - wikidata_riveraltname
    diff_set2 = set(diff_set).difference(set(wikidata_riveraltname))
    print("---opendata_rivername - wikidata_rivername - wikidata_riveraltname, count=%i---" %(len(diff_set2)))
    print( "\n".join(diff_set2))

    #in wikidata_rivername but not in opendata_rivername
    diff_set = set(wikidata_rivername).difference(set(opendata_rivername))
    print("---in wikidata_rivername but not in opendata_rivername---")
    print( "\n".join(diff_set))
    
    #in (wikidata_rivername and opendata_rivername)
    union_set = set(wikidata_rivername).intersection(set(opendata_rivername))
    print("---in (wikidata_rivername and opendata_rivername)---")
    print( "\n".join(union_set))


