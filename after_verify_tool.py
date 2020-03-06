#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import os
import shutil
import subprocess
import time
import tarfile
import requests
import urllib
import json
import paramiko
from RAW_DEF import *

def build_dic_replacement():
	os.rename(DEST_DATA_PATH + '/Data_POI/Local/PoiData_All.txt', DEST_DATA_PATH + '/Data_POI/Local/PoiData_All.txt.original')
	
	try:
		run_script = "/user1/sireroot/src/RawDataVerifyTool/binary/BuildDicReplmt " + \
				DEST_DATA_PATH + '/Data_POI/Local/PoiData_All.txt.original ' \
				+ DEST_DATA_PATH + '/Data_POI/Normalization_Dic.txt ' + \
				DEST_DATA_PATH + '/Data_POI/Local/PoiData_All.txt'
		subprocess.call(run_script, shell=True)
	except Exception as e:
		print(e)
		sys.exit(1)

def make_tgzfile(outfile, src_dir):
	try:
		with tarfile.open(outfile, "w:gz", compresslevel=6) as tar:
			tar.add(src_dir, arcname=os.path.basename(src_dir))
	except Exception as e:
		print(e)
		sys.exit(1)

def send_sftp_file(param_json):
	ret_list = list()
	dest_dict = json.loads(param_json)

	for d in dest_dict:
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


if __name__ == '__main__':
	##PoiData_All.txt 치환사전 적용
	print("PoiData_All.txt replacement dic")
	#build_dic_replacement()

	##압축 말고
	print("Compress tgz file")
	#make_tgzfile(DEST_TGZ_FILE, DEST_DATA_PATH)

	##각 서버로 전송
	print("sftp send each sever")
	send_sftp_file(DELIVER_SERVER_INFO)
