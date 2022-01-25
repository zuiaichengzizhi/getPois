# Author: Ghost Zou
"""
说明：
    版本3中将多个key组合在一起，发现程序很容易卡死，不知道什么原因
    因此，版本4决定通过矩形编号记录爬取到了那个矩形，已经爬取过的矩形，直接进入队列。
"""

import traceback

import requests
import pymysql
from DBUtils.PooledDB import PooledDB
import time
import queue
import math
import socket
from concurrent.futures import ThreadPoolExecutor

''' 
    https://restapi.amap.com/v3/place/polygon?polygon=113.705304,31.366201|115.089744,29.974252&key=10518669c9fd0a0532e41189d61e1e9b&extensions=all&types=010000&offset=10&page=90
    key1:10518669c9fd0a0532e41189d61e1e9b
    key2:5b5982ae83581df80086e296a723e0c1
    key3:9dbe5d62f27ab94ed7295e0bc3154926
'''
# 初始区域的经纬度
lon_l = 113.705304
lan_l = 31.366201
lon_r = 115.089744
lan_r = 29.974252

# 要爬取POI的类型
types = '010000|020000|030000|040000|050000|060000|' \
        '070000|080000|090000|100000|110000|120000|' \
        '130000|140000|150000|160000|170000|180000|' \
        '190000|200000|210000|220000|230000|240000|' \
        '970000|990000'

# 获取POI的接口
key1 = '10518669c9fd0a0532e41189d61e1e9b'  # 未使用
key2 = '5b5982ae83581df80086e296a723e0c1'  # 未使用
key3 = '9dbe5d62f27ab94ed7295e0bc3154926'  # 未使用
key4 = 'b2b75693666e9411773e5db2fcab89e2'  # 未使用
key5 = 'f55fdc5abe855a404d0106c84bfd58d4'  # 未使用
key = key4
url = 'https://restapi.amap.com/v3/place/polygon?' \
      'polygon={lon_l},{lan_l}|{lon_r},{lan_r}' \
      '&key={key}&extensions=all' \
      '&types={types}&offset=25&page={page}'
url = url.format(lon_l="{lon_l}", lan_l="{lan_l}",
                 lon_r="{lon_r}", lan_r="{lan_r}",
                 key=key,
                 types="{types}", page="{page}")

# rec编号
rec_id = 0


# point
class Point:
    """
    用经纬度来描述一个点
    """

    def __init__(self, lon, lan):
        self.lon = lon  # 经度
        self.lan = lan  # 纬度


# area
class Rectangle:
    """
    用左上角的经纬度和右下角的经纬度描述一个矩形区域
    """
    isGet = "False"
    isSmall = "False"
    id = 0

    def __init__(self, p_l, p_r):
        self.p_l = p_l  # 左上角的点
        self.p_r = p_r  # 右下角的点

    # 返回4个四等分的小矩形
    def split(self):
        global rec_id
        # 经纬度差
        lon_d = (self.p_r.lon - self.p_l.lon) / 2
        lan_d = (self.p_l.lan - self.p_r.lan) / 2

        # 四个小矩形区域
        rec_A = Rectangle(
            Point(self.p_l.lon, self.p_l.lan),
            Point(float('%.6f' % (self.p_l.lon + lon_d)),
                  float('%.6f' % (self.p_r.lan + lan_d))),
        )
        rec_id = rec_id + 1
        rec_A.id = rec_id

        rec_B = Rectangle(
            Point(float('%.6f' % (self.p_l.lon + lon_d)),
                  self.p_l.lan),
            Point(self.p_r.lon,
                  float('%.6f' % (self.p_r.lan + lan_d)))
        )
        rec_id = rec_id + 1
        rec_B.id = rec_id

        rec_C = Rectangle(
            Point(self.p_l.lon,
                  float('%.6f' % (self.p_r.lan + lan_d))),
            Point(float('%.6f' % (self.p_l.lon + lon_d)),
                  self.p_r.lan)
        )
        rec_id = rec_id + 1
        rec_C.id = rec_id

        rec_D = Rectangle(
            Point(float('%.6f' % (self.p_l.lon + lon_d)),
                  float('%.6f' % (self.p_r.lan + lan_d))),
            Point(self.p_r.lon, self.p_r.lan)
        )
        rec_id = rec_id + 1
        rec_D.id = rec_id

        # 将矩形放入队列和数据库
        return [rec_A, rec_B, rec_C, rec_D]


# 初始矩形的左上点和右下点
p_l = Point(lon_l, lan_l)
p_r = Point(lon_r, lan_r)

# ##############################################################
# 初始矩形，內含两点，左上，右下
# ###############################################################
# 武汉
rec = Rectangle(p_l, p_r)

# 创建数据库连接池和线程池
print("正在创建数据连接池，请稍等...")
db_pool = PooledDB(pymysql, 1, host='localhost', user='root', port=3306,
                   passwd='111111', db='poi', use_unicode=True)
print("连接池创建成功，连接数30，下面创建线程池...")

thread_pool = ThreadPoolExecutor(max_workers=1)
print("线程池创建成功，最大线程数15")


def open_html(real_url):
    """解析url，获取数据
    :param real_url: api地址
    :return: url中包含的数据
    """
    while True:
        try:
            data = requests.get(real_url).json()
            return data
        except socket.timeout:
            print("NET_STATURS IS NOT GOOD")
        except Exception as e:
            traceback.print_exc()


def get_pois(rec: Rectangle, url: str, data_count: int, city: str):
    """获取区域数据
    :param rec: 目标区域
    :param url: api地址
    :param data_count: poi数量
    """
    url = url.format(lon_l=rec.p_l.lon, lan_l=rec.p_l.lan,
                     lon_r=rec.p_r.lon, lan_r=rec.p_r.lan,
                     types=types, page="{page}")
    f_url = open("url.txt", "a", encoding="UTF-8")
    data = open_html(url.format(page=0))
    if data["status"] == "1":
        # 执行到这里，说明url中含有poi数据,下面开始爬取
        all_page = math.ceil(data_count / 25)

        # 从数据库连接池获取连接
        conn = db_pool.connection()
        cursor = conn.cursor()

        # 这里开始该api的每一页进行爬取
        for page in range(all_page):
            # 从第1页开始到第最后一页
            url_real = url.format(page=page + 1)
            f_url.write(url_real + '\n')
            data = open_html(url_real)
            if data["status"] == "1":  # 判断第page+1页是否有内容
                pois = data["pois"]

                # 将数据储存在数据库
                '''
                 优化点：1.0版本 db先创建好，一次用完不关，最后再关
                         2.0版本 使用数据库连接池和多线程
                '''

                # 向poi表中中插入poi点的sql语句
                str_sql = 'insert into t_poi_{_city}(id,name,address,typecode,lon,' \
                          'lan,pcode,pname,citycode,cityname,adcode,adname)' \
                          ' values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(__city=city)
                count = 0
                for poi in pois:
                    count += 1
                    value = [poi['id'], poi['name'], poi["address"],
                             poi['typecode'], poi['location'].split(',')[0],
                             poi['location'].split(',')[1], poi['pcode'],
                             poi['pname'], poi['citycode'], poi['cityname'],
                             poi['adcode'], poi['adname']]

                    if [] in value:
                        value[value.index([])] = ''
                    try:
                        cursor.execute(str_sql, value)
                        conn.commit()
                    except Exception as e:
                        # 打印日志文件
                        print(e)
                        f = open("wrong.txt", 'a', encoding="UTF-8")
                        time_fomat = '%Y-%m-%d %X'
                        time_current = time.strftime(time_fomat)
                        f.write(time_current + '  第{page}页,第{index}个出错\n'
                                .format(page=page, index=count))

        # 该区域数据爬取结束，标记矩形为已爬取，更新数据库，并将连接放回连接池
        rec.isGet = 'True'
        str_sql = "update t_poi_{_city} set isget=%s " \
                  "where lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s".format(__city=city)
        value = [rec.isGet, str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan)]
        cursor.execute(str_sql, value)
        conn.commit()
        cursor.close()
        conn.close()
    else:
        print("当前key在要爬取一个区域时用光")


def crawl_pois(recs, url: str, city: str):
    """分析一个区域的POI数量，多于800则将区域四等分，加入队列
    :param recs: 初始队列
    :param url:
    :param city:城市名称
    :return:
    """
    count = 0  # 计算进行了几次分析的计数器，判断程序是否卡死
    with open("count.txt", 'a', encoding="UTF-8") as f_count:
        while not recs.empty():
            rec = recs.get()
            f_count.write("这是第{_count}次运行分析一个区域\n".format(_count=count))

            if recs_count > rec.id:
                # 到这里说明，目前队列里的矩形都已经入过库了
                conn = db_pool.connection()
                cursor = conn.cursor()

                # sql语句，查询当前矩形的poi是否小于800
                str_sql = 'select issmall from t_rec_{_city} ' \
                          'where lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s'.format(_city=city)
                value = [str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan)]
                cursor.execute(str_sql, value)
                result = cursor.fetchone()[0]

                if result == "False":
                    temp = rec.split
                    for r in temp:
                        recs.put(r)
                else:
                    str_sql = 'select isget from t_rec_{_city} ' \
                              'where lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s'.format(_city=city)
                    value = [str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan)]
                    cursor.execute(str_sql, value)
                    result = cursor.fetchone()[0]

                    if result=="True":
                        {}
                    else:
                        thread_pool.submit(get_pois, rec, url, data_count, city)


            elif recs_count <= rec_id or recs_count :
                # 到这里说明此时取出的矩形尚未入库

                # 根据矩形区域的经纬度坐标，拼接url
                url_real = url.format(lon_l=rec.p_l.lon, lan_l=rec.p_l.lan,
                                      lon_r=rec.p_r.lon, lan_r=rec.p_r.lan,
                                      types=types, page="{page}")

                # 获取url返回的数据
                data = open_html(url_real.format(page=0))
                if data["status"] == "1":
                    # 执行到这里，说明url中含有poi数据,下面开始判断区域POI数量
                    data_count = int(data['count'])
                    if data_count > 800:

                        # 将矩形放入队列和数据库
                        temp = rec.split()

                        conn = db_pool.connection()
                        cursor = conn.cursor()
                        str_sql_insert = 'insert into t_rec_{_city} ' \
                                         '(lon_l,lan_l,lon_r,lan_r,issmall,isget)' \
                                         'values' \
                                         '(%s,%s,%s,%s,%s,%s)'.format(_city=city)

                        for r in temp:
                            # 将小矩形放进队列
                            recs.put(r)
                            value_insert = [str(r.p_l.lon), str(r.p_l.lan), str(r.p_r.lon), str(r.p_r.lan),
                                            r.isSmall, r.isGet]
                            # 将小矩形放入数据库
                            cursor.execute(str_sql_insert, value_insert)
                            conn.commit();

                        conn.close();
                        cursor.close();
                    else:
                        '''
                        # 判断该矩形区域的数据是否爬取过
                        conn = db_pool.connection()
                        cursor = conn.cursor()
    
                        str_sql = 'select isget from t_rec_{_city} ' \
                                  'where lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s'.format(_city=city)
                        value = [str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan)]
                        cursor.execute(str_sql, value)
                        result = cursor.fetchone()[0];
                        if result == "False":
                        '''
                        rec.isSmall = "True"
                        conn = db_pool.connection()
                        cursor = conn.cursor()
                        str_sql_insert = 'insert into t_rec_{_city} ' \
                                         '(lon_l,lan_l,lon_r,lan_r,issmall,isget)' \
                                         'values' \
                                         '(%s,%s,%s,%s,%s,%s)'.format(_city=city)
                        value_insert = [str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan),
                                        rec.isSmall, rec.isGet]
                        cursor.execute(str_sql_insert, value_insert)
                        # 使用一个线程来获取poi数据
                        thread_pool.submit(get_pois, rec, url, data_count, city)
                        # get_pois(rec, url, data_count)

                else:
                    print("出错")

            count += 1


'''
    初始化数据
'''
# ####################################城市
# 郑州
rec_zhengzhou = Rectangle(
    Point(112.726469, 34.983807),
    Point(114.244241, 34.271630)
)

# ##################################矩形数量
conn = db_pool.connection()
cursor = conn.cursor()
str_sql = 'SELECT COUNT(id) FROM t_rec_zhengzhou'
cursor.execute(str_sql)
recs_count = cursor.fetchone()[0]

if __name__ == '__main__':
    recs = queue.LifoQueue()
    recs.put(rec_zhengzhou)
    crawl_pois(recs, url, 'zhengzhou')
