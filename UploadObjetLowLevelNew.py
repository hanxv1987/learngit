'分段上传内容到S3上'

import os;
import math;
import time;
import boto3;

#-------------------------------------------------------------------------
#需要设置的内容
bucket = 'copy-tokyo';             #要存储内容的bucket名称
srcPath = 'F:\\Tools';              #原文件存储位置，该路径名称最好和存储桶实际存储位置一致，便于做映射
                                   #例如S3中要存放到TEST文件夹内，那么原文件就放置到本地根目录下的TEST文件夹内

#可固定的参数
logDir  = 'F:\\Log';           #统计操作
logFile = 'runLog.txt';        #运行日志


#其他参数
fileName = list();       #文件名称
filePath = list();       #文件路径（含名称，例F:\\TEST\\test.txt）
dstPath  = list();       #bucket对应的存储位置（含名称，例TEST/test.txt）


#-------------------------------------------------------------------------
#函数部分

#获取时间
def GetSysTime ():
     Time = time.strftime( '%Y-%m-%d %H:%M:%S  ', time.localtime(time.time()) );
     #测试
     #print( Time );
     return (Time);


#设置操作日志存储位置
def SetOperateRecord ( path ):
     if False == os.path.exists( path ) :
          os.makedirs( path );
     if False == os.path.exists( path ) :
          print('Err: 操作记录存储位置无法创建' );
          return (1);
     else :
          return (0);


#设置运行日志记录
def OpenLogFile (fileDir, logName):
     if (None == fileDir) or (None == logName) :
          print( 'Err: 无法打开日志文件' );
          return (None);
     else :
          fullPath = fileDir + '\\' + logName;
          fp = open(fullPath, 'a');
          return (fp);


#写日志操作
def WriteLog (fp, info):
     fp.write (GetSysTime() + info);


#退出日志操作
def CloseLog (fp):
     if fp:
          fp.close();


#遍历所有文件并完成初始化
def GetAllFilePath (path, fp) :
     if ('' == path) or (False == os.path.exists (path)):
          info = 'Err:输入的文件存储路径有误，程序退出\n';
          print( info );
          WriteLog (fp, info);
          CloseLog (fp);
          exit();
     else :
          WriteLog (fp, '***待上传的文件列表***\n');
          for rt, dit, files in os.walk( path ) :
               for f in files :
                    fileName.append( f );
                    filePath.append( os.path.join(rt, f) );
                    fp.write(os.path.join(rt, f) + '\n');


#设置文件的存储路径
def SetSaveFilePath (filePathList, fp) :
     if 0 == len(filePathList) :
          info = 'Err:没有文件需要上传，程序退出\n';
          WriteLog (fp, info);
          CloseLog (fp);
          exit();
     else :
          for each in filePathList :
               part  = each.split(':', 1);
               dstPath.append( part[1].replace("\\", "/")[1:] );


#设置分块上传的参数
def InitMultiPart (s3resource, bucketName, fileName):
     return ( s3resource.create_multipart_upload (Bucket = bucketName, Key = fileName) );


#设置一个文件的分块数
def SetChunkNum (file, chunkSize = 5242880):
     fileSize = os.path.getsize ( file );
     return ( int( math.ceil(int(fileSize)/chunkSize)) );


#上传一个文件，fileName为含路径的全称
def UploadOneObject (srcPath, dstPath, bucket, S3Resource, chunkSize, logRecord) :
     parts = [];      #分块信息统计
     etags = {};      #分块编码标记
     state = 0;       #异常状态标记
     body  = None;    #当前数据块
     fileSize   = os.path.getsize ( srcPath );
     chunkcount = int( math.ceil(int(fileSize)/chunksize) );
     multipart_upload = S3Resource.create_multipart_upload (Bucket = bucket, Key = dstPath);


     with open(srcPath, 'rb') as fp :
          part_number = 1;
          while True:
               offset = chunksize * (part_number - 1);
               bytes_ = min (chunksize, int(fileSize) - offset);

               try :
                    if 0 == state :
                         body = fp.read(bytes_);
                    part = s3.upload_part(Bucket = bucket,
                                          Key = dstPath,
                                          PartNumber = part_number,
                                          UploadId = multipart_upload['UploadId'],
                                          Body = body);
                    state = 0;                 
                    info = '第' + str(part_number) + '块传输完成\n';
                    WriteLog (log, info);
                    print ('第' + str(part_number) + '块传输完成');

                    part_info = {'PartNumber':part_number, 'ETag':part['ETag']};
                    parts.append(part_info);

                    part_number += 1;
                    if part_number > chunkcount:
                         break;
               except Exception:  #遇到上传故障时重试
                    state = 1;    #异常时为1
                    WriteLog (log, 'Err:第' + str(part_number) + '块上传时出现错误\n');
                    print('Err:第' + str(part_number) + '块上传时出现错误');
                    continue;

     s3.complete_multipart_upload(Bucket = bucket,
                                  Key = dstPath,
                                  UploadId = multipart_upload['UploadId'],
                                  MultipartUpload = {'Parts': parts});
               
     
#-----------------------------------------------------------------------------
#程序运行部分
if '__main__' == __name__ :
     print ('**********************************************');
     print ('程序启动\n');
     
     #参数部分
     log = None;  #写日志文件
     partETags = [];
     s3 = None;
     chunksize = 16777216;  #分块的大小 (16MB)


     #程序启动，日志记录
     if not SetOperateRecord ( logDir ):
          log = OpenLogFile (logDir, logFile);
     log.write ('**********************************************\n');
     WriteLog (log, "程序启动，日志启动\n");
     GetAllFilePath (srcPath, log);
     SetSaveFilePath (filePath, log);

     #以单个文件的形式上传数据
     s3 = boto3.client ( 's3' );
     num = 0;        #当前第几个文件
     sus = list();   #成功的文件
     for each in filePath :  #遍历每一个文件
          log.write('\n');
          WriteLog (log, '***上传文件 ' + each + '***\n');
          print ( '***上传文件 ' + each + '***' );
          UploadOneObject (each, dstPath[num], bucket, s3, chunksize, log);
          num += 1;
          sus.append (num);
          WriteLog (log, '***上传文件 ' + each + '完成***\n');
          print ( '***上传文件 ' + each + '完成***\n' );
          

     log.write('\n***统计信息***\n');
     info = '全部文件数目：' + str(len(filePath)) + ' 上传成功文件数目：' + str(num) + '， 失败文件数目：' + str(len(filePath) - num) + '\n\n';
     WriteLog (log, info);
     log.write('***上传成功文件***\n');
     print ('***统计信息***');
     print (info);
     for i in range (len(sus)):
           log.write(str(filePath[i]) + '\n');

     log.write('\n');
     WriteLog (log, "程序运行结束\n");
     log.write ('**********************************************\n\n');
     log.close();
     print ('程序运行结束');
     print ('**********************************************\n');

