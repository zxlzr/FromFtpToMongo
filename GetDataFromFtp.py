#!/usr/bin/env python 
#coding=utf-8
import ftplib
import os
import datetime
import paramiko 
#yestday计算昨天时间，给出格式化字符串
fte=datetime.datetime.now()
fte1=fte + datetime.timedelta(days = -1)
print datetime.datetime.strftime( fte, '%Y%m%d')
yest=datetime.datetime.strftime( fte1, '%Y%m%d')
print yest
#设置ftp目录
#dirn = '/home/DATA/2009032800/' 
#dir1 = '/home/DATA/'+yest+'12'
#file1 = 'BN_MOVERS_LQ_20140104.TXT' 
# Define the local directory name to put data in
ddir="/home/datadir"
# If directory doesn't exist make it
#if not os.path.isdir(ddir):
 #   os.mkdir(ddir)
# Change the local directory to where you want to put the data
#os.chdir(ddir)

# login to FTP这里设置地址用户名密码




# ftp settings
ftp_server   = "lz3.verizon.com"
ftp_port     = 22
ftp_user     = "sftpBridgevine"
ftp_password = "!6CePdP5Vn#4$"
 #def sftp_stor_files(file_zip, destFile):
t = paramiko.Transport((ftp_server, ftp_port))
t.connect(username=ftp_user, password=ftp_password, hostkey=None)
sftp = paramiko.SFTPClient.from_transport(t)
    
    # dirlist on remote host
dirlist = sftp.listdir('.')
print "Dirlist:", dirlist
    
   # sftp.put(file_zip, destFile)

t.close()











# change the remote directory
#f.cwd(dir1)

# define filename
#循环拼需要ftp的文件名
#ii=0
#file0 = yest+'12'
#temp1=''
#while ii<=20:
  #  temp1=12*ii
  #  temp2=str(temp1)
   # if ii<9:
   #     file1=file0+'0'+temp2+'.grb1' 
   #     if ii==0:file1=file0+'00'+temp2+'.grb1' 
  #  else:
    #    file1=file0+temp2+'.grb1'    
   # ii=ii+1     
#    print ii	 




