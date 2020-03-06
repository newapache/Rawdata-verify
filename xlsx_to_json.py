#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import pandas as pd


## 정의서 추출
Definition = pd.ExcelFile('MIW_SIRE_Raw_Data_정의_4_5_2.xlsx')

# sheet Data_Addr
Data_Addr = Definition.parse(0, skiprows=4, skipfooter=21).dropna(axis=1, how='all')

# sheet Data_Admin
AdminCode = Definition.parse(1, skiprows=15, skipfooter=96).dropna(axis=1, how='all')
AdminCode_Short = Definition.parse(1, skiprows=30, skipfooter=82).dropna(axis=1, how='all')
adm_city = Definition.parse(1, skiprows=44, skipfooter=69).dropna(axis=1, how='all')
adm_gu = Definition.parse(1, skiprows=56, skipfooter=53).dropna(axis=1, how='all')
adm_dong =  Definition.parse(1, skiprows=72, skipfooter=29).dropna(axis=1, how='all')
adm_ri =  Definition.parse(1, skiprows=96, skipfooter=12).dropna(axis=1, how='all')

#sheet Data_POI
CCTV = Definition.parse(2, skiprows=18, skipfooter=139).dropna(axis=1, how='all')
CateCode =  Definition.parse(2, skiprows=44, skipfooter=125).dropna(axis=1, how='all')
ExtraName =  Definition.parse(2, skiprows=58, skipfooter=117).dropna(axis=1, how='all')
PoiData_All =  Definition.parse(2, skiprows=65, skipfooter=91).dropna(axis=1, how='all')
PoiDetail =  Definition.parse(2, skiprows=91, skipfooter=64).dropna(axis=1, how='all')
PoiSubData =  Definition.parse(2, skiprows=123, skipfooter=43).dropna(axis=1, how='all')
PoiSubData_Multi = Definition.parse(2, skiprows=139, skipfooter=37).dropna(axis=1, how='all')
APT =  Definition.parse(2, skiprows=171, skipfooter=0).dropna(axis=1, how='all')

#sheet Data_Ranking
adinfo_01 =  Definition.parse(3, skiprows=11, skipfooter=16).dropna(axis=1, how='all')
landmark =  Definition.parse(3, skiprows=18, skipfooter=11).dropna(axis=1, how='all')

#sheet Data_Road
Data_Road =  Definition.parse(4, skiprows=4, skipfooter=21).dropna(axis=1, how='all')

#sheet Data_TEL
Data_TEL =  Definition.parse(6, skiprows=4, skipfooter=21).dropna(axis=1, how='all')

#sheet Data_Theme
PoiData = Definition.parse(7, skiprows=14, skipfooter=85).dropna(axis=1, how='all')
PoiDataDetail = Definition.parse(7, skiprows=40, skipfooter=56).dropna(axis=1, how='all')
ThemeCode = Definition.parse(7, skiprows=68, skipfooter=40).dropna(axis=1, how='all')
ThemeDetailInfo = Definition.parse(7, skiprows=84, skipfooter=13).dropna(axis=1, how='all')
ThemePoiRaw = Definition.parse(7, skiprows=111, skipfooter=8).dropna(axis=1, how='all')
ThemeZoneVertex = Definition.parse(7, skiprows=116, skipfooter=0).dropna(axis=1, how='all')



## make dictionary
def_dict = {}

# sheet Data_Addr
def_dict['Data_Addr'] = Data_Addr.to_dict('records')

# sheet Data_Admin
def_dict['AdminCode.txt'] = AdminCode.to_dict('records')
def_dict['AdminCode_Short.txt'] = AdminCode_Short.to_dict('records')
def_dict['adm_city.txt'] = adm_city.to_dict('records')
def_dict['adm_gu.txt'] = adm_gu.to_dict('records')
def_dict['adm_dong.txt'] = adm_dong.to_dict('records')
def_dict['adm_ri.txt'] = adm_ri.to_dict('records')

#sheet Data_POI
def_dict['CCTV.txt'] = CCTV.to_dict('records')
def_dict['CateCode.txt'] = CateCode.to_dict('records')
def_dict['ExtraName.txt'] = ExtraName.to_dict('records')
def_dict['PoiData_All.txt'] = PoiData_All.to_dict('records')
def_dict['PoiDetail.txt'] = PoiDetail.to_dict('records')
def_dict['PoiSubData.txt'] = PoiSubData.to_dict('records')
def_dict['PoiSubData_Multi.txt'] = PoiSubData_Multi.to_dict('records')
def_dict['복합브랜드아파트.txt'] = APT.to_dict('records')

#sheet Data_Ranking
def_dict['adinfo_01.txt'] = adinfo_01.to_dict('records')
def_dict['landmark.txt'] = landmark.to_dict('records')

#sheet Data_Road
def_dict['Data_Road'] = Data_Road.to_dict('records')

#sheet Data_TEL
def_dict['Data_TEL'] = Data_TEL.to_dict('records')


#sheet Data_Theme
def_dict['PoiData.txt'] = PoiData.to_dict('records')
def_dict['PoiDataDetail.txt'] = PoiDataDetail.to_dict('records')
def_dict['ThemeCode.txt'] = ThemeCode.to_dict('records')
def_dict['ThemeDetailInfo.txt'] = ThemeDetailInfo.to_dict('records')
def_dict['ThemePoiRaw.txt'] = ThemePoiRaw.to_dict('records')
def_dict['ThemeZoneVertex.txt'] = ThemeZoneVertex.to_dict('records')


## save json file
with open('MIW_SIRE_Raw_Data_정의_4_5_2.json', 'w') as fp:
    json.dump(def_dict, fp, indent=4, ensure_ascii=False)
