# 音频处理
from threading import Thread
from queue import Queue
from ftplib import FTP

import subprocess
import os
import mslex 
import time
import datetime
import pymysql
import ffmpeg


class action():
    def __init__(self):
        self._upload_file_action = False
        self._base_past = os.path.realpath('')
        self._input_path = "%s/%s" % (self._base_past, 'old_mp3')#高质量
        self._ret_path = "%s/%s" % (self._base_past, 'new_mp3')#低质量

        #判断文件夹是否存在
        if not os.path.exists(self._ret_path):
            os.makedirs(self._ret_path)

        now = datetime.datetime.now()
        self._base_sql_mp3_play = '/down/%s/%s/%s' % (now.year, now.month, now.day)#数据库存的，同时也是服务器路径（高质量）
        self._base_sql_mp3_past = '/play/%s/%s/%s' % (now.year, now.month, now.day)#数据库存的，同时也是服务器路径（低质量）
        self._base_sql_mp3_wav = '/wav/%s/%s/%s' % (now.year, now.month, now.day)  # 数据库存的，同时也是服务器路径（低质量）
        self._base_sql_mp3_flac = '/flac/%s/%s/%s' % (now.year, now.month, now.day)  # 数据库存的，同时也是服务器路径（低质量）
        self._base_sql_mp3_bowen = '/bowen/%s/%s/%s' % (now.year, now.month, now.day)
        self._base_sql_mp3_pic = '/pic/%s/%s/%s' % (now.year, now.month, now.day)
        # self._base_sql_mp3_pic = '/pic/%s/%s/%s' % (now.year, now.month, now.day)

        # 连接数据库
        self._conn = pymysql.connect(
            host="hk-cynosdbmysql-grp-2mpy5vnd.sql.tencentcdb.com",
            port=20213,
            user="root",
            password="Asd1231230.0",
            database="mixupload"
        )
        # 创建游标
        self._cursor = self._conn.cursor()

        #pdf配置
        self._ftp_host = '127.0.0.1'
        self._ftp_name = 'djxiaokai'
        self._ftp_password = '4fGkcbADGi56nkkC'
        self._ftp_path = '/'

        # 创建一个FTP连接
        self._ftp = FTP(self._ftp_host)

        # 登录到FTP服务器
        self._ftp.login(self._ftp_name, self._ftp_password)
        self._ftp.encoding = 'utf-8'


        self.process_num = 16 #进程数
        #需要处理的队列
        self._file_queue = Queue()
        
        for dirpath, dirnames, filenames in os.walk(self._base_past):
            for filename in filenames:
                if '.mp3' in filename:
                    file_path = os.path.join(dirpath, filename)  # 文件路径
                    self._file_queue.put([file_path, filename])
        
        for _ in range(self.process_num):
            #创建队列进行下载
            Thread(target=self._save).start()
            time.sleep(0.5)


    def _save(self):
        while not self._file_queue.empty():
            #循环需要的队列
            #1.进行音频压缩

            file_path, file_name  = self._file_queue.get()
            
            print(file_path)
            cid = file_path.replace(self._base_past, '').split('\\')[2]
            print(cid)

            #判断文件夹是否存在
            if not os.path.exists(self._ret_path +'/'+ cid):
                os.makedirs(self._ret_path +'/'+ cid)
            new_file_path = "%s/%s/%s" % (self._ret_path, cid, file_name)#新文件路径
            
            cmd_str = r'ffmpeg -i %s -b:a 64k %s' % (mslex.quote(file_path), mslex.quote(new_file_path))
            # os.system(cmd_str)
            process = subprocess.Popen(cmd_str, shell=True)
            process.wait()  # 等待子进程执行完毕
            process.terminate()  # 关闭子进程
            cmd_str = r'ffmpeg -i %s -f wav %s' % (mslex.quote(file_path), mslex.quote(new_file_path.replace('.mp3', '.wav')))
            process = subprocess.Popen(cmd_str, shell=True)
            process.wait()  # 等待子进程执行完毕
            process.terminate()  # 关闭子进程
            cmd_str = r'ffmpeg -i %s -f flac %s' % (
            mslex.quote(file_path), mslex.quote(new_file_path.replace('.mp3', '.flac')))
            process = subprocess.Popen(cmd_str, shell=True)
            process.wait()  # 等待子进程执行完毕
            process.terminate()  # 关闭子进程


            #4.生成波纹
            time.sleep(0.5)
            bowen_path_name = new_file_path.replace('.mp3','')+'_bowen.png'
            cmd_str = r'ffmpeg -i %s -filter_complex "showwavespic=s=977x86:colors=0x808080" -frames:v 1 %s' % (mslex.quote(new_file_path), mslex.quote(bowen_path_name))
            # os.system(cmd_str)
            process = subprocess.Popen(cmd_str, shell=True)
            process.wait()  # 等待子进程执行完毕
            process.terminate()  # 关闭子进程
            time.sleep(0.5)

            pic_path_name = new_file_path.replace('.mp3', '') + '_pic.png'
            cmd_str = r'ffmpeg -i %s -an -y -vcodec copy -frames:v 1 %s' % (mslex.quote(new_file_path), mslex.quote(pic_path_name))
            # os.system(cmd_str)
            process = subprocess.Popen(cmd_str, shell=True)
            process.wait()  # 等待子进程执行完毕
            process.terminate()  # 关闭子进程

            #5.村数据库
            timestamps = time.time()
            time1, bit_rate, size = self.get_audio_info(file_path)
            print(time1, bit_rate, size, timestamps)

            # SQL语句，向表中插入一行数据

            sql = "INSERT INTO v41_dance (`name`,dx, yz, sc, purl, bpm, diao, cid, durl, addtime, fid, 	wav, flac,pic,bowen) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s)"

            filename_arr = file_name.split('-')
            # 数据值
            name = filename_arr[2].strip() + ' - ' + filename_arr[3].strip() if len(filename_arr) > 3 else \
            filename_arr[2].strip()
            print(name)
            
            values = (name.replace(".mp3",""),
                      size, bit_rate, time1,
                      self._base_sql_mp3_past +'/'+cid+'/'+ (file_name.replace("\\","/")),
                        filename_arr[1].strip(), filename_arr[0].strip(), cid,
                       self._base_sql_mp3_play +'/'+cid+'/'+ (file_name.replace("\\","/")), timestamps, 1,
                      self._base_sql_mp3_wav +'/'+cid+'/'+ (file_name.replace("\\","/").replace(".mp3",".wav")),
                      self._base_sql_mp3_flac +'/'+cid+'/'+ (file_name.replace("\\","/").replace(".mp3",".flac")),
                      self._base_sql_mp3_pic + '/' + cid + '/' + (file_name.replace("\\", "/").replace(".mp3", "_pic.png")),
                      self._base_sql_mp3_bowen + '/' + cid + '/' + (file_name.replace("\\", "/").replace(".mp3", "_bowen.png")),
                      )

            # 执行SQL语句
            self._conn.ping(reconnect=True)
            self._cursor.execute(sql, values)
            # 提交事务
            self._conn.commit()
            time.sleep(0.5)
            #3.上传到服务器
            #高质量
            self.upload_file(file_path, self._ftp_path + self._base_sql_mp3_play +'/'+cid+'/'+ file_name)
            #低质量
            self.upload_file(new_file_path, self._ftp_path + self._base_sql_mp3_past +'/'+cid+'/'+ file_name)
            self.upload_file(new_file_path.replace(".mp3",".wav"), self._ftp_path + self._base_sql_mp3_wav +'/'+cid+'/'+ file_name.replace(".mp3",".wav"))
            self.upload_file(new_file_path.replace(".mp3",".flac"), self._ftp_path + self._base_sql_mp3_flac +'/'+cid+'/'+ file_name.replace(".mp3",".flac"))
            #波纹
            self.upload_file(bowen_path_name, self._ftp_path + self._base_sql_mp3_bowen +'/'+cid+'/'+ file_name.replace('.mp3','')+'_bowen.png')
            self.upload_file(pic_path_name, self._ftp_path + self._base_sql_mp3_pic +'/'+cid+'/'+ file_name.replace('.mp3','')+'_pic.png')


            time.sleep(0.5)
    
    #上传
    def upload_file(self, local_file, remote_file):
        while 1:
            if not self._upload_file_action:
                self._upload_file_action = True
                # 获取远程文件夹的名称
                folder_name = os.path.dirname(remote_file)
                print(folder_name)

                # 检查文件夹是否存在
                try:
                    self._ftp.cwd(folder_name)  # 尝试切换到指定路径
                    self._ftp.cwd('/')  # 切换回根目录
                except:
                    # 如果文件夹不存在，创建它
                    self.make_ftp_dir(folder_name)

                # 将本地文件打开并以二进制模式读取
                with open(local_file, 'rb') as f:
                    # 使用storbinary方法将文件内容写入到远程位置
                    self._ftp.storbinary(f'STOR {remote_file}', f)
                
                #上传后删除本地文件
                os.remove(local_file)
                self._upload_file_action = False
                break
            else:
                time.sleep(0.5)

    #ftp创建目录
    def make_ftp_dir(self, path):
        dirs = path.split('/')
        self._ftp.cwd('/')  # 切换回根目录
        for d in dirs:
            try:
                self._ftp.mkd(d)
                self._ftp.cwd(d)
            except Exception as e:
                print(f"Failed to create directory {d}: {e}")
                self._ftp.cwd(d)  # 如果目录已存在，就切换到该目录
        self._ftp.cwd('/')  # 切换回根目录

     # 获取音频时长
    def get_audio_info(self, audio_path):
        print(audio_path)
        audio_file = ffmpeg.probe(audio_path)
        # print(audio_file)
        duration = float(audio_file['format']['duration'])  # 时间秒
        # 将秒转换为分秒
        minutes, seconds = divmod(duration, 60)
        bit_rate = "%sKbps" % int(int(audio_file['format']['bit_rate']) / 1000)  # 音质
        size = "%sM" % round(int(audio_file['format']['size']) / 1048576, 2)  # 大小 M

        return [('%s:%s' % (int(minutes), int(seconds))), bit_rate, size]

if __name__ == "__main__":
    action()

