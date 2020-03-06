#!/usr/bin/python
# -*- coding:utf-8 -*-

import time
import datetime

CUR_DATE = input("검증할 파일 날짜를 입력하세요ex)20190101 : ")
PREV_DATE = input("이전 파일 날짜를 입력하세요 : ")

##slack token
token = ''

## File Path
ORG_DATA_PATH = '/data3/service_user/liteam/Upload/DATASET_%s' % CUR_DATE
PREV_PATH = '/data1/sireroot/DATASET/DATASET_%s' % PREV_DATE

EUCKR_DATA_PATH = '/data1/sireroot/DATASET/DATASET_%s_euckr' % CUR_DATE
DEST_DATA_PATH = '/data1/sireroot/DATASET/DATASET_%s' % CUR_DATE
DEST_DATA_BASE_PATH = '/data1/sireroot/DATASET/'
TGZ_FILE = 'DATASET_%s.tgz' % CUR_DATE
DEST_TGZ_FILE = DEST_DATA_BASE_PATH + TGZ_FILE
NEW_PATH = DEST_DATA_PATH

## Date
now = datetime.datetime.today()
yest = datetime.datetime.today() - datetime.timedelta(1)

DTODAY = now.strftime('%Y%m%d')
DYESTERDAY = yest.strftime('%Y%m%d')

T_YEAR = '%04d/' % now.year
T_MONTH = '%02d/' % now.month

Y_YEAR = '%04d/' % yest.year
Y_MONTH = '%02d/' % yest.month

## host information 
LEIA = '' 
YODA = ''
LUKE = ''
DEVNODEM = ''

DELIVER_SERVER_INFO = LEIA + ',' + YODA + ',' + LUKE + ',' + DEVNODEM
