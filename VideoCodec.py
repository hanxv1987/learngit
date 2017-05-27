"说明：用于进行批量视频转码并拷贝的制定目录下，具有简单的日志统计功能\
（1）FilesDir为素材存放目录，ProceSaveDir为转码后的视频存放目录，FileCopyDir为指定的拷贝目录\
（2）需要修改转码参数时，在ProcessVideo函数内修改cmd1和cmd2两个参数即可\
（3）若是转码后想保存在根目录下，例如存放在F盘，则ProceSaveDir = 'F:\\' \
（4）ProceSaveDir和FileCopyDir目录下建议为空文件夹"

import os;
import glob;
import time;
import shutil;


#需要配置的项
FilesDir     = "I:\\测试"       #所有要处理的介质存放路径
ProceSaveDir = "F:\\成品"    #处理后的文件存放目录
FileCopyDir  = "I:\\成品"        #处理完的文件要拷贝的目录

logDir       = "F:\\Log";           #日志文件夹
logFile      = "runLog.txt"         #日志文件
logWrite     = None;                #写日志操作

FileList = list();  #视频文件列表
DirList  = list();  #视频文件存储路径列表（含文件名）
DirList1 = list();  #视频文件存储路径列表（不含文件名）
Errlist  = list();  #未正确处理的文件列表
SaveList = list();  #存储文件路径列表（含文件名）


#获取时间
def GetSysTime ():
     Time = time.strftime( '%Y-%m-%d %H:%M:%S  ', time.localtime(time.time()) );
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
def OpenLogFile (fileDir, logName) :
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


#关闭日志
def CloseLog ( fp ):
     fp.close();


#得到文件和文件路径
def GetAllFilePath (path, fp) :
     if ('' == path) or (False == os.path.exists (path)):
          info = 'Err:输入的文件存储路径有误，程序退出\n';
          print( info );
          WriteLog (fp, info);
          CloseLog (fp);
          exit();
     else :
          for rt, dit, files in os.walk( path ) :
               for f in files :
                    FileList.append( f );
                    DirList.append( os.path.join(rt, f) );
                    DirList1.append( rt );


#设置处理后的文件存储路径,srcDirList不含文件名,save表示变量ProceSaveDir,vFormat表示转码后的视频格式，如‘.mp4’
def SetProcessFilePath (srcDirList, srcFileList, save, vFormat, fp):
     i = len( srcDirList );
     if 0 == i:
          info = 'Err:没有文件需要转码\n';
          print( info );
          WriteLog (fp, info);
          CloseLog (fp);
          exit();
     if False == os.path.exists ( save ) :
          if 1 == os.makedirs ( save ) :
               info = 'Err:无法创建保存成品的文件夹\n';
               print( info );
               WriteLog (fp, info);
               CloseLog (fp);
               exit(); 
               
     for j in range( i ) :
          (file, ext) = os.path.splitext( srcFileList[j] );
          file += vFormat;
          dirFile = srcDirList[j].split(':')[1:];
          dirPath = "";
          for each in dirFile:
               dirPath += each;
          saveDir = save + dirPath;
          if False == os.path.exists( saveDir ) :  #创建文件存储路径
               os.makedirs( saveDir );
          fullPath = saveDir + '\\' + file;
          SaveList.append( fullPath );
          

#拷贝数据
def CopyData(srcDir, dstDir, fp) :
     if srcDir == dstDir :
          print('数据无需拷贝');
          return 0;

     if False == os.path.exists( srcDir ):
          info = 'Err:要拷贝的文件路径不存在\n';
          print( info );
          WriteLog (fp, info);
          CloseLog (fp);
          exit();
     if False == os.path.exists( dstDir ) :
          if 1 == os.makedirs( dstDir ) :
               info = 'Err:无法创建拷贝文件的目标文件夹\n';
               print( info );
               WriteLog (fp, info);
               CloseLog (fp);
               exit();
     shutil.move(srcDir, dstDir);


#视频处理
def ProcessVideo(srcDirList, dstDirList, fp) :
     cmd1 = 'ffmpeg -i ';
     cmd2 = ' -c:v h264_nvenc -b:v 1000k -maxrate 1500k -c:a aac -b:a 64k -f mp4 ';
     num  = 0;
     if (0 == len(srcDirList)) or (0 == len(dstDirList)) or (len(srcDirList) != len(dstDirList)):
          info = 'Err:文件不存在或信息不对，无法转码\n';
          print( info );
          WriteLog (fp, info);
          CloseLog (fp);
          exit();

     i = len(srcDirList);
     for j in range(i):
          cmd = cmd1 + srcDirList[j] + cmd2 + dstDirList[j];
          if 1 == os.system( cmd ) :
               print('Err: %s转码错误' %srcDirList[j]);
               Errlist.append( srcDirList[j] );
          else :
               num += 1;
     return (num);
          

#主程序运行
if __name__ == '__main__' :
     info = '******************程序运行******************';
     print(info);

     num = 0;
     logWrite = OpenLogFile(logDir, logFile);
     WriteLog (logWrite, info + '\n');
          
     GetAllFilePath(FilesDir, logWrite);
     SetProcessFilePath (DirList1, FileList, ProceSaveDir, '.mp4', logWrite);
     num = ProcessVideo(DirList, SaveList, logWrite);
     CopyData(ProceSaveDir, FileCopyDir, logWrite);

     info = '******************处理完成******************'
     print(info);
     WriteLog (logWrite, info + '\n');
     info = '共处理文件数量: ' + str(len(FileList)) + ' 成功数量: ' + str(num);
     print(info);
     WriteLog (logWrite, info + '\n');
     
     if len(ProceSaveDir) - num > 0 :
          WriteLog (logWrite, '\n');
          info = '失败文件列表:'
          print(info);
          WriteLog (logWrite, info + '\n');

          i = len( Errlist );
          for j in range (i):
               print( Errlist[j] );
               WriteLog (logWrite, Errlist[j] + '\n');
          WriteLog (logWrite, '\n');

     info = '******************程序结束******************';
     print(info);
     WriteLog (logWrite, info + '\n\n');
     CloseLog ( logWrite );
               
          
                 
     
      
