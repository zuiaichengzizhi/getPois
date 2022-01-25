# Author: Ghost Zou
"""
    将key集成在一起,一个key的额度用完之后，可以再继续换下一个key
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
keys = [key3, key4, key5]

url = 'https://restapi.amap.com/v3/place/polygon?' \
      'polygon={lon_l},{lan_l}|{lon_r},{lan_r}' \
      '&key={key}&extensions=all' \
      '&types={types}&offset=25&page={page}'
'''
url = url.format(lon_l="{lon_l}", lan_l="{lan_l}",
                 lon_r="{lon_r}", lan_r="{lan_r}",
                 key=key,
                 types="{types}", page="{page}")
'''

# 初始区域的经纬度
lon_l = 113.705304
lan_l = 31.366201
lon_r = 115.089744
lan_r = 29.974252


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
    id = 0
    flag = "False"

    def __init__(self, p_l, p_r):
        self.p_l = p_l  # 左上角的点
        self.p_r = p_r  # 右下角的点


# 初始矩形的左上点和右下点
p_l = Point(lon_l, lan_l)
p_r = Point(lon_r, lan_r)

# ##############################################################
# 初始矩形，內含两点，左上，右下
# ###############################################################
# 武汉
rec_wuhan = Rectangle(p_l, p_r)

# 郑州　 112.726469,34.27163;114.244241,34.983807
# 112.726469,34.983807;114.244241,34.271630
rec_zhengzhou = Rectangle(
    Point(112.726469, 34.983807),
    Point(114.244241, 34.271630)
)

# 创建数据库连接池和线程池
print("正在创建数据连接池，请稍等...")
db_pool = PooledDB(pymysql, 10, host='localhost', user='root', port=3306,
                   passwd='111111', db='poi', use_unicode=True)
print("连接池创建成功，连接数30，下面创建线程池...")

thread_pool = ThreadPoolExecutor(max_workers=10)
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


def get_pois(rec: Rectangle, url: str, data_count: int, area: str):
    """获取区域数据
    :param rec: 目标区域
    :param url: api地址
    :param data_count: poi数量
    :param area: 地区
    """
    '''
    url = url.format(lon_l=rec.p_l.lon, lan_l=rec.p_l.lan,
                     lon_r=rec.p_r.lon, lan_r=rec.p_r.lan,
                     types=types, page="{page}")
    '''
    f_url = open("url_{area}.txt".format(area=area), "a", encoding="UTF-8")
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

                str_sql = 'insert into t_poi_{area}(id,name,address,typecode,lon,' \
                          'lan,pcode,pname,citycode,cityname,adcode,adname)' \
                          ' values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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
                        cursor.execute(str_sql.format(area=area), value)
                        conn.commit()
                    except Exception as e:
                        # 打印日志文件
                        print(e)
                        f = open("wrong_{area}.txt".format(area=area), 'a', encoding="UTF-8")
                        time_fomat = '%Y-%m-%d %X'
                        time_current = time.strftime(time_fomat)
                        f.write(
                            time_current + 'lon_l={lon_l}, lan_l={lan_l} | lon_r={lon_r}, lan_r={lan_r}  第{page}页,第{index}个出错\n'
                            .format(lon_l=rec.p_l.lon, lan_l=rec.p_l.lan, lon_r=rec.p_r.lon, lan_r=rec.p_r.lan,
                                    page=page + 1, index=count))

        # 该区域数据爬取结束，标记矩形为爬取过，更新数据库，并将连接放回连接池
        rec.flag = 'True'
        str_sql = "update t_rec_{area} set flag=%s " \
                  "where lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s"
        value = [rec.flag, str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan)]
        cursor.execute(str_sql.format(area=area), value)
        conn.commit()
        cursor.close()
        conn.close()
    else:
        print("在获取数据时出现异常")


def crawl_pois(recs, url: str, area: str):
    """分析一个区域的POI数量，多于800则将区域四等分，加入队列
    :param recs: 初始队列
    :param url:
    :param table:表名
    :return:
    """
    count = 0  # 计算进行了几次分析的计数器，判断程序是否卡死
    with open("count.txt", 'a', encoding="UTF-8") as f_count:
        # keys的索引
        i = 0
        while not recs.empty():
            rec = recs.get()
            f_count.write("这是第{_count}次运行分析一个区域\n".format(_count=count))

            # 根据矩形区域的经纬度坐标，拼接url
            url_real = url.format(lon_l=rec.p_l.lon, lan_l=rec.p_l.lan,
                                  lon_r=rec.p_r.lon, lan_r=rec.p_r.lan,
                                  key=keys[i],
                                  types=types, page="{page}")

            # 获取url返回的数据
            data = open_html(url_real.format(page=0))
            flag = True
            while flag:
                if data["status"] == "1":
                    # 执行到这里，说明url中含有poi数据,下面开始判断区域POI数量
                    data_count = int(data['count'])

                    # 判断一个区域的数据是否大于800
                    if data_count > 800:

                        # 经纬度差
                        lon_d = (rec.p_r.lon - rec.p_l.lon) / 2
                        lan_d = (rec.p_l.lan - rec.p_r.lan) / 2

                        # 四个小矩形区域
                        rec_A = Rectangle(
                            Point(rec.p_l.lon, rec.p_l.lan),
                            Point(float('%.6f' % (rec.p_l.lon + lon_d)),
                                  float('%.6f' % (rec.p_r.lan + lan_d)))
                        )
                        rec_B = Rectangle(
                            Point(float('%.6f' % (rec.p_l.lon + lon_d)),
                                  rec.p_l.lan),
                            Point(rec.p_r.lon,
                                  float('%.6f' % (rec.p_r.lan + lan_d)))
                        )
                        rec_C = Rectangle(
                            Point(rec.p_l.lon,
                                  float('%.6f' % (rec.p_r.lan + lan_d))),
                            Point(float('%.6f' % (rec.p_l.lon + lon_d)),
                                  rec.p_r.lan)
                        )
                        rec_D = Rectangle(
                            Point(float('%.6f' % (rec.p_l.lon + lon_d)),
                                  float('%.6f' % (rec.p_r.lan + lan_d))),
                            Point(rec.p_r.lon, rec.p_r.lan)
                        )

                        # 将矩形放入队列和数据库
                        temp = [rec_A, rec_B, rec_C, rec_D]

                        conn = db_pool.connection()
                        cursor = conn.cursor()
                        str_sql_insert = 'insert into t_rec_{area} ' \
                                         '(lon_l,lan_l,lon_r,lan_r,flag)' \
                                         'values' \
                                         '(%s,%s,%s,%s,%s)'

                        str_sql_select = 'select id from t_rec_{area} ' \
                                         'where ' \
                                         'lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s'

                        for r in temp:
                            # 将小矩形放进队列
                            recs.put(r)

                            # 查询当前矩形是否已经插入数据库
                            value_select = [str(r.p_l.lon), str(r.p_l.lan), str(r.p_r.lon), str(r.p_r.lan)]
                            cursor.execute(str_sql_select.format(area=area), value_select)
                            result_select = cursor.fetchone()
                            if result_select:
                                # 如果已经放入了数据库，则判断下一个小矩形
                                {}
                            else:
                                # 如果为放进，则放进
                                value_insert = [str(r.p_l.lon), str(r.p_l.lan), str(r.p_r.lon), str(r.p_r.lan), r.flag]
                                cursor.execute(str_sql_insert.format(area=area), value_insert)
                                conn.commit();

                        conn.close();
                        cursor.close();
                    else:
                        # 判断该矩形区域的数据是否爬取过
                        conn = db_pool.connection()
                        cursor = conn.cursor()

                        str_sql = 'select flag from t_rec_{area} ' \
                                  'where lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s'
                        value = [str(rec.p_l.lon), str(rec.p_l.lan), str(rec.p_r.lon), str(rec.p_r.lan)]
                        cursor.execute(str_sql.format(area=area), value)
                        result = cursor.fetchone()[0];

                        if result == "False":
                            # 使用一个线程来获取poi数据
                            thread_pool.submit(get_pois, rec, url_real, data_count, area)
                    flag=False
                else:
                    if i < len(keys):
                        i += 1
                    else:
                        print("key已用尽")
                        return

        count += 1


if __name__ == '__main__':
    recs = queue.LifoQueue()
    total_id = 0

    '''
         recs.put(rec_wuhan) # 武汉
    '''  # 已爬取的城市

    recs.put(rec_zhengzhou)

    crawl_pois(recs, url, "zhengzhou")
