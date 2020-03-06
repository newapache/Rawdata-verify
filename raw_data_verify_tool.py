#!/usr/bin/python
# -*- coding:utf-8 -*-


import os
import re
import sys
import time
import json
import shutil
import tarfile
import paramiko
import datetime
import subprocess
import requests
import urllib
from slacker import Slacker
from RAW_DEF import *

def_dict = {}
with open('MIW_SIRE_Raw_Data_정의서_4_5_2.json') as f:
	def_dict = json.load(f)

def send_slack_message(msg):
	slack = Slacker(token)
	slack.chat.post_message('#dataset-verification', msg)

def send_message(hostname):
	sender = ''
	recv = ''
	msg = '[Success] %s to %s Transfer Completed.' % (TGZ_FILE, hostname)
	msg_encode = urllib.quote(msg)
	url = '' % (sender, recv, msg_encode)
	r = requests.get(url)


def build_files_set(rootdir):
    root_to_subtract = re.compile(r'^.*?' + rootdir + r'[\\/]{0,1}')
    files_set = set()
    for (dirpath, dirnames, filenames) in os.walk(rootdir):
        for filename in filenames + dirnames:
            full_path = os.path.join(dirpath, filename)
            relative_path = root_to_subtract.sub('', full_path, count=1)
            files_set.add(relative_path)
    return files_set

def compare_directories(dir1, dir2):
    files_set1 = build_files_set(dir1)
    files_set2 = build_files_set(dir2)
    return (files_set1 - files_set2, files_set2 - files_set1)

def get_all_files(path):
    res = []
    for root, dirs, files in os.walk(path):
        rootpath = os.path.join(os.path.abspath(path), root)
        for file in files:
            filepath = os.path.join(rootpath, file)
            res.append(filepath)
    return res

def dataset_file_check():
    in_dir1, in_dir2 = compare_directories(PREV_PATH, NEW_PATH)
    if len(in_dir1) != 0:
        print('\nRemoved files from new dataset.')
        print('Files only in {}:'.format(PREV_PATH))
        for relative_path in in_dir1:
            print('* {0}'.format(relative_path))
    else:
        print('\nRemoved files from new dataset.')
        print('Not Founded.')

    if len(in_dir2) != 0:
        print('\nAdded files from new dataset.')
        print('Files only in {}:'.format(NEW_PATH))
        for relative_path in in_dir2:
            print('* {0}'.format(relative_path))
    else:
        print('\nAdded files from new dataset.')
        print('Not Founded.')

def compare_file_size():
    prev_dict = dict()
    new_dict = dict()

    normal_dict = dict()
    abnormal_dict = dict()
    prev_zero_dict = dict()
    relevant_dict = dict()

    res1 = get_all_files(PREV_PATH)
    for r in res1:
        k = r.replace(PREV_PATH, "")
        prev_dict[k] = os.path.getsize(r)

    res2 = get_all_files(NEW_PATH)

    for r in res2:
        new_key = r.replace(NEW_PATH, "")
        new_val = float(os.path.getsize(r))
        new_dict[new_key] = new_val

        if new_key in prev_dict:
            prev_val = float(prev_dict.get(new_key))
            if new_val == prev_val:
                normal_dict[new_key] = new_val
            elif prev_val == 0:
                prev_zero_dict[new_key] = new_val
            elif new_val/prev_val <= 0.98 or new_val/prev_val >= 1.4:
                abnormal_dict[new_key] = new_val
            else:
                relevant_dict[new_key] = new_val


    print("Same File Size - {0} files".format(len(normal_dict)))
    for k in normal_dict:
        print("* {0} {1}".format(k, normal_dict[k]))

    print("\nRelevant File Size Changed - {0} files".format(len(relevant_dict)))
    for k in relevant_dict:
        print("* {0} {1}".format(k, relevant_dict[k]))

    print("\nAbnormal Estimated File Size Changed - {0} files".format(len(abnormal_dict)))
    for k in abnormal_dict:
        print("* {0} {1}".format(k, abnormal_dict[k]))

    print("\nPrevious Dataset File Size is 0 - {0} files".format(len(prev_zero_dict)))
    for k in prev_zero_dict:
        print("* {0} {1}".format(k, prev_zero_dict[k]))


def dir_invalid_check(code_dict, dir_name, col_no):
    print("[" + dir_name + "]")
    filename_list = os.listdir(NEW_PATH + dir_name)
    for filename in filename_list:
        invalid_dict = dict()
        with open(NEW_PATH + dir_name + filename, "r") as f:
            lines = f.readlines()
        for line in lines:
            res = line.split('|')
            if res[col_no] not in code_dict:
                if res[col_no] not in invalid_dict:
                    invalid_dict[res[col_no]] = 1
                else:
                    invalid_dict[res[col_no]] = invalid_dict[res[col_no]] + 1
        print(dir_name + filename + " - " + str(len(invalid_dict)) + " invalid records founded.")
        invalid_dict_sorted_keys = sorted(invalid_dict, key=invalid_dict.get, reverse=True)
        for key in invalid_dict_sorted_keys:
            print(key, invalid_dict[key])


def file_invalid_check(code_dict, filename, col_no):
    invalid_dict = dict()
    print("[" + filename + "]")
    with open(NEW_PATH + filename, "r") as f:
        lines = f.readlines()
    for line in lines:
        res = line.split('|')
        if res[col_no] not in code_dict:
            if res[col_no] not in invalid_dict:
                invalid_dict[res[col_no]] = 1
            else:
                invalid_dict[res[col_no]] = invalid_dict[res[col_no]] + 1

    print(filename + " - " + str(len(invalid_dict)) + " invalid records founded.")
    invalid_dict_sorted_keys = sorted(invalid_dict, key=invalid_dict.get, reverse=True)
    for key in invalid_dict_sorted_keys:
        print(key, invalid_dict[key])

def file_delimiter_check(filename, del_no):
	invalid_dict = dict()

	with open(NEW_PATH + filename, "r") as f:
		lines = f.readlines()
	for line in lines:
		res = line.split('|')
		if len(res) != del_no:
			print("Field Count is Abnormal. NORMAL[%s] vs ABNORMAL[%s]" % len(res), del_no)
			print(line)


def admcode_check():
    admcode_dict = dict()

    with open(NEW_PATH + '/Data_Admin/AdminCode_Short.txt') as f:
        short_lines = f.readlines()

    with open(NEW_PATH + '/Data_Admin/AdminCode.txt') as f:
        lines = f.readlines()

    if len(short_lines) != len(lines):
        print('AdminCode_Short.txt and AdminCode.txt records is not same.')
        sys.exit(0)

    for line in lines:
        res = line.split('|')
        admcode_dict[res[0]] = res[0]

    dir_invalid_check(admcode_dict, "/Data_Addr/", 0)
    dir_invalid_check(admcode_dict, "/Data_Road/", 0)
    dir_invalid_check(admcode_dict, "/Data_TEL/", 11)
    #dir_invalid_check(admcode_dict, "/Data_TEL_LGU/", 11)
    file_invalid_check(admcode_dict, "/Data_POI/Local/PoiData_All.txt", 11)
    file_invalid_check(admcode_dict, "/Data_POI/CCTV.txt", 11)
    file_invalid_check(admcode_dict, "/Data_POI/PoiSubData.txt", 8)
    file_invalid_check(admcode_dict, "/Data_Admin/adm_vertex.txt", 0)
    file_invalid_check(admcode_dict, "/Data_Theme/PoiData.txt", 11)


def catecode_check():
    catecode_dict = dict()
    with open(NEW_PATH + '/Data_POI/CateCode.txt') as f:
        lines = f.readlines()
    for line in lines:
        res = line.split('|')
        catecode_dict[res[0]] = res[0]

    dir_invalid_check(catecode_dict, "/Data_TEL/", 15)
	#dir_invalid_check(catecode_dict, "/Data_TEL_LGU/", 15)
    file_invalid_check(catecode_dict, "/Data_POI/Local/PoiData_All.txt", 15)
    file_invalid_check(catecode_dict, "/Data_POI/CCTV.txt", 15)
    file_invalid_check(catecode_dict, "/Data_POI/PoiSubData.txt", 9)
    file_invalid_check(catecode_dict, "/Data_POI/VVMS.txt", 15)
    file_invalid_check(catecode_dict, "/Data_Theme/PoiData.txt", 15)

def encoding_utf8(src_file, dest_file):

	if dest_file.endswith('.TXT'):
		dest_file.replace('.TXT', '.txt')

	if dest_file.endswith('.CSV'):
		dest_file.replace('.CSV', '.csv')

	outfile = open(dest_file, 'w')

	with open(src_file, 'r', encoding="cp949") as f:
		for line in f:
			outfile.write(line)
	outfile.close()

def get_all_raw_files(path):
	res = list()
	res_dir = list()

	for root, dirs, files in os.walk(path):
		rootpath = os.path.join(os.path.abspath(path), root)
		res_dir.append(rootpath)

		for file in files:
			filepath = os.path.join(rootpath, file)
			res.append(filepath)
	return res, res_dir


def encode_dataset(src_path, dest_path):
	res, res_dir = get_all_raw_files(src_path)
	for r in res_dir:
		dir_elem = r.replace(src_path, dest_path)
		if not os.path.exists(dir_elem):
			os.makedirs(dir_elem)

	dest_list = list()
	for src_file in res:
		dest_file = src_file.replace(src_path, dest_path)
		encoding_utf8(src_file, dest_file)
		short_dest_file = dest_file.replace(dest_path, '')
		print("[%s] .... OK" % short_dest_file)

def file_deli_cnt_check(filename):
	with open(filename, 'r') as f:
		lines = f.readlines()
	print('[' + filename + ']')
	cnt = 1
	for line in lines:
		arr = line.split('|')
		if len(arr) != len(lines[0].split('|')):
			print(cnt, line)
		cnt += 1


def delimiter_check():
	## 파일 리스트 가져오기
	file_list = list()
	for root, dirs, files in os.walk(DEST_DATA_PATH):
		for fname in files:
			full_fname = os.path.join(root, fname)
			if not (fname.endswith('adm_vertex.txt') or fname.endswith('iNaviLive현황.xls')):
				file_list.append(full_fname)

	for f in file_list:
		## file의 첫번째 행의 필드 개수와 나머지 레코드가 같은지 비교
		file_deli_cnt_check(f)

def make_tgzfile(outfile, src_dir):
	try:
		with tarfile.open(outfile, "w:gz", compresslevel=6) as tar:
			tar.add(src_dir, arcname=os.path.basename(src_dir))
	except Exception as e:
		print(e)
		sys.exit(1)


def get_poi_rank_file():
	file_name = 'poi_rank_%s.csv' % DTODAY
	file_name_pop = 'popular_plog_%s.txt' % DTODAY

	base_file = '/data2/sireroot/BULKLOAD/' + T_YEAR + T_MONTH + file_name
	base_file_pop = '/data2/sireroot/BULKLOAD/' + T_YEAR + T_MONTH + file_name_pop

	dest_file = DEST_DATA_PATH + '/Data_Ranking/poi_rank.csv'
	dest_file_pop = DEST_DATA_PATH + '/Data_Ranking/popular_plog.txt'

	ssh  = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect('', username='', password='')

	sftp = ssh.open_sftp()

	try:
		sftp.stat(base_file)
		print("get %s to POI_Rank" % base_file)
		sftp.get(base_file, dest_file)
		print("get %s to POI_POP" % base_file_pop)
		sftp.get(base_file_pop, dest_file_pop)

	except:
		file_name = 'poi_rank_%s.csv' % DYESTERDAY
		base_file = '/data2/sireroot/BULKLOAD/' + Y_YEAR + Y_MONTH + file_name
		print("get %s to POI_Rank" % base_file)
		sftp.get(base_file, dest_file)

		file_name_pop = 'popular_plog_%s.txt' % DTODAY
		base_file_pop = '/data2/sireroot/BULKLOAD/' + T_YEAR + T_MONTH + file_name_pop
		print("get %s to POI_POP" % base_file_pop)
		sftp.get(base_file_pop, dest_file_pop)

	ssh.close()

def extraname_field_check():
	extra_file_name = DEST_DATA_PATH + '/Data_POI/ExtraName.txt'

	with open(extra_file_name, 'r') as f:
		lines = f.readlines()

	for line in lines:
		arr = line.split('|')
		tmp = arr[3].split(',')

		wrongCount = ',,'

		if len(tmp) != int(arr[2]) or wrongCount in arr[3] or arr[3].startswith(',') or arr[3][-2] == ',':
			print(arr[0], arr[2], len(tmp))
			print(arr[3])
			print("\n")


def send_sftp_file(param_json):
	ret_list = list()
	dest_dict = json.loads(param_json)
	print(dest_dict)
	for d in dest_dict:
		print(dest_dict[d]['host'])

		host_name = dest_dict[d]['host']
		user_name = dest_dict[d]['user']
		passwd = dest_dict[d]['passwd']
		dest = dest_dict[d]['dest']

		ssh  = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.connect(host_name, username=user_name, password=passwd)

		sftp = ssh.open_sftp()
		print(DEST_TGZ_FILE)
		print(dest + '/' +TGZ_FILE)

		try:
			print("sftp put tgz file to %s" % d)
			sftp.put(DEST_TGZ_FILE, dest + '/' +TGZ_FILE)

		except:
			print("[Error] sftp put to %s send error." % d)
		cmd = 'tar zxf %s -C %s' % (dest+'/'+TGZ_FILE, dest)
		print(cmd)
		print('[' + d + ']')

		if d is not 'DevNodeM' or d is not 'luke':
			stdin, stdout, stderr = ssh.exec_command(cmd)
			res = stdout.readlines()
			if d is not 'DevNodeM':
				ret_list.append(d)	

		ssh.close()

	return ret_list

def dokdo_pos_check():
	file_1 = DEST_DATA_PATH + "/Data_Admin/AdminCode.txt"
	file_2 = DEST_DATA_PATH + "/Data_POI/Local/PoiData_All.txt"

	cmd1 = "grep '^4794025027|' %s" % file_1
	cmd2 = "grep '^1114635' %s" % file_2
	cmd3 = "grep '^2064132|' %s" % file_2

	result = subprocess.check_output(cmd1, shell=True)
	res1 = result.decode('utf-8').split('|')
	if res1[5] == '549270' and res1[6] == '498881':
		print("독도리 행정코드 좌표 --> [OK]")
	else:
		print("독도리 행정코드 확인 필요 Normal(549270, 498881) vs Current(%s, %s)" % (res1[5], res1[6]))

	result = subprocess.check_output(cmd2, shell=True)
	res2 = result.decode('utf-8').split('|')
	if res2[7] == '549276' and res2[8] == '498844':
		print("독도(우리땅) POI 좌표 --> [OK]")
	else:
		print("독도리 행정코드 확인 필요 Normal(549276, 498844) vs Current(%s, %s)" % (res1[7], res1[8]))

	result = subprocess.check_output(cmd3, shell=True)
	res3 = result.decode('utf-8').split('|')
	if res3[7] == '549270' and res3[8] == '498881':
		print("독도리 POI 좌표 --> [OK]")
	else:
		print("독도리 행정코드 확인 필요 Normal(549270, 498881) vs Current(%s, %s)" % (res1[7], res1[8]))

## PoiDetail의 특정 필드 확인해주기
def examination_field():
	#파일을 DEST_DATA_PATH 을 타고들어가 경로에 있는 txt파일을 read로 연다
	file_open = open(DEST_DATA_PATH + '/Data_POI/PoiDetail.txt','r')
	#txt파일을 한줄씩 list로 읽어준다
	lines = file_open.readlines()

	#특정 라인을 입력해주기 위한 두개의 파일을 write로 열어준다
	field_num = open(DEST_DATA_PATH + '/result_field_num.txt','w')
	field_fax = open(DEST_DATA_PATH + '/result_field_fax.txt','w')
	full_path3 = [DEST_DATA_PATH + '/result_field_num.txt',DEST_DATA_PATH + '/result_field_fax.txt']
	

	for line in lines:
		words_list = line.split('|')
		#만약 4번째 필드가 전화로 시작하지 않거나 공백아 아닐경우에
		#field_num에서 지정해준 txt에 한줄씩 저장시킨다
		if not words_list[3].startswith('전화') and words_list[3] != '':
			field_num.write(line)
		elif not words_list[4].startswith('전화') and words_list[4] != '':
			field_num.write(line)
		elif not words_list[5].startswith('팩스') and words_list[5] != '':
			field_fax.write(line)
		elif not words_list[6].startswith('팩스') and words_list[6] != '':
			field_fax.write(line)
		else:
			pass
	field_num.close()
	field_fax.close()
	#새로운 파일을 열어줌으로써 field_num과 field_fax의 결과를 하나의 txt파일로 만들어 준다
	with open(DEST_DATA_PATH + '/result_PoiDetail_field.txt','w') as outfile:
		for fname in full_path3:
			with open(fname) as infile:
				for line in infile:
					outfile.write(line)\

	#result_field의 총 라인 갯수를 출력해준다
	cnt_line = open(DEST_DATA_PATH + '/result_PoiDetail_field.txt','r')
	result_line = cnt_line.readlines()

	for line in result_line:
		words_list = line.split('|')
		print(words_list)
	print(len(result_line))

	cnt_line.close()

	#result_field_num과 fax는 이제 필요가 없어졌으로 삭제해준다
	os.remove(DEST_DATA_PATH + '/result_field_num.txt')
	os.remove(DEST_DATA_PATH + '/result_field_fax.txt')
	os.remove(DEST_DATA_PATH + '/result_PoiDetail_field.txt')

	file_open.close()

def compare_type(fullpath, file_def_dict):
	print('[', fullpath, ']')
	
	with open(fullpath, 'r') as f:
		lines = f.readlines()

	cnt = 1
	for line in lines:
		arr = line.strip().split('|')
		field_num = 0
		for field in arr:
			try :
				if file_def_dict[field_num]['data type'] == 'integer' and field:
					isinstance(int(field), int)

			except ValueError :
				print('Type of field is different from the definition. line : ', cnt, ' ', field_num+1, fullpath)
			except IndexError :
				print('Num of fields is different from the definition. line : ', cnt)
			field_num += 1
		cnt += 1


def field_type_check():
	for root, dirs, files in os.walk(DEST_DATA_PATH):
		for fname in files:
			fullpath = os.path.join(root, fname)
			upper = fullpath.split('/')[-2]
			if fname in def_dict:
				compare_type(os.path.join(root, fname), def_dict[fname])
			elif upper in def_dict:
				compare_type(os.path.join(root, fname), def_dict[upper])

def is_null_field(fullpath, file_def_dict):
	print('[', fullpath, ']')

	with open(fullpath, 'r') as f:
		lines = f.readlines()

	cnt = 1
	for line in lines:
		arr = line.strip().split('|')
		field_num = 0
		for field in arr:
			try :
				if file_def_dict[field_num]['필수 여부'] == 'O' and not field:
					print('Required field is missing. line:', cnt, ' field:', file_def_dict[field_num]['항목'])
			except IndexError :
				print('Num of fields is different from the definition. line : ', cnt)
			field_num += 1
		cnt += 1

def null_check():
	for root, dirs, files in os.walk(DEST_DATA_PATH):
		for fname in files:
			fullpath = os.path.join(root, fname)
			upper = fullpath.split('/')[-2]
			if fname in def_dict:
				is_null_field(os.path.join(root, fname), def_dict[fname])
			elif upper in def_dict:
				is_null_field(os.path.join(root, fname), def_dict[upper])


def check_record_size():
	file_path = DEST_DATA_PATH + '/Data_POI/Local/PoiData_All.txt'

	with open(file_path, 'r') as f:
		lines = f.readlines()

	emptystring = ''
	cnt = 1
	for line in lines:
		if sys.getsizeof(line) >= 4096:
			print('Record size over 4 Kbyte. line : ', cnt, line)

		if (sys.getsizeof(line.split('|')[2]) - sys.getsizeof(emptystring)) >= 90:
			print('Field size over 90 Byte. line : ', cnt, line.split('|')[2])
		cnt += 1

def comma_semicolone_check():
	with open('PoiSubData_Multi.txt','r') as f1:
		lines = f1.readlines()

	for line in lines:
		word_split = line.split('|')
		word_semi = word_split[2].count(';')
		word_comma = word_split[2].count(',')

		if word_comma - word_semi != 1:
			print(line)


if __name__ == '__main__':
	print('RawDataVerification Start')
	start = time.time()

	###### A. Raw File copy / Encoding Process ######
	## A.1. 원본 파일 복사
	print("Copy to EUCKR_DATA_PATH[%s] completed.\n\n\n\n\n" % EUCKR_DATA_PATH)
	shutil.copytree(ORG_DATA_PATH, EUCKR_DATA_PATH)
	print("Copy to EUCKR_DATA_PATH[%s] completed.\n\n\n\n\n" % EUCKR_DATA_PATH)
	
	## A.2. UTF-8로 변환
	print("Encoding UTF-8 Start")
	encode_dataset(EUCKR_DATA_PATH, DEST_DATA_PATH)
	print("Encoding Completed\n\n\n\n\n")


	## A.4. poi_rank 복사후 rename
	print("Get poi_rank file from PDB Server")
	get_poi_rank_file()
	print("Get poi_rank Completed\n\n\n\n\n")

	###### B. Verification Process ######

	## delimeter Check Add
	print("Delimiter Check")
	delimiter_check()
	print("Delimiter Check Completed\n\n\n\n\n")

	## 독도 좌표 확인 로직 추가...
	print("Dokdo Position Check")
	dokdo_pos_check()

	## 정의서 데이터 타입과의 일치 여부 검증
	print("Type of field Check")
	field_type_check()
	print("Type of field Check Completed\n\n\n\n\n")

	# ## 필수 필드 Null Check
	print("null Check")
	null_check()
	print("null Check Completed\n\n\n\n\n")

	# ## record 4kB 이상, 필드 90B 이상 체크
	print("\n\nRecord Size Check")
	check_record_size()
	print("Record Size count Check Completed\n\n\n\n\n")

	## ExtraName.txt 파일의 콤마 딜리미터 개수 체크 로직 추가
	print("ExtraName field count Check")
	extraname_field_check()
	print("ExtraName field count Check Completed\n\n\n\n\n")

	print("DataSet File Check")
	dataset_file_check()
	print("DataSet File Check COmpleted\n\n\n\n")
	
	## 이전 데이터셋과 파일 사이즈 비교
	print("Compare File Size between Previous Dataset and New Dataset.")
	compare_file_size()

	## 비정상 행정코드 검출
	print("Abnormal AdmCode Check")
	admcode_check()

	## 비정상 종별코드 검출
	print("Abnormal CateCode Check")
	catecode_check()
	
	## PoiDetail 필드 검사
	print("Examination PoiDetail.txt field Start!!!")
	examination_field()
	print("Examination PoiDetail.txt field Complete\n\n\n\n\n")

	## PoiSubData_Multi.txt 필드 검사
	print("Check PoiSubData_Multi.txt comma and semicolon!!!")
	comma_semicolone_check()
	print("Check PoiSubData_Multi.txt comma and semicolon Complete\n\n\n\n")

	end = time.time()
	elapsed = end - start
	print('\n\nRawDataVerification Completed')
	print('Elapsed Time: ' + str(elapsed))
