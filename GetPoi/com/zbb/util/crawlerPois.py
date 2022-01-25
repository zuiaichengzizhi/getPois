import requests
import pymysql
import time
import math
import socket

''' 
    https://restapi.amap.com/v3/place/polygon?polygon=113.705304,31.366201|115.089744,29.974252&key=10518669c9fd0a0532e41189d61e1e9b&extensions=all&types=010000&offset=10&page=90
    key1:10518669c9fd0a0532e41189d61e1e9b
    key2:5b5982ae83581df80086e296a723e0c1
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
url = 'https://restapi.amap.com/v3/place/polygon?' \
     'polygon={lon_l},{lan_l}|{lon_r},{lan_r}' \
     '&key=5b5982ae83581df80086e296a723e0c1&extensions=all' \
     '&types={types}&offset=25&page={page}'


# point
class Point:
    """
    用经纬度来描述一个点
    """
    def __init__(self, lon, lan):
        self.lon = lon      # 经度
        self.lan = lan      # 纬度


# area
class Rectangle:
    """
    用左上角的经纬度和右下角的经纬度描述一个矩形区域
    """
    def __init__(self, p_l, p_r):
        self.p_l = p_l      # 左上角的点
        self.p_r = p_r      # 右下角的点


# 初始矩形的左上点和右下点
p_l = Point(lon_l,lan_l)
p_r = Point(lon_r,lan_r)

# 初始矩形，內含两点，左上，右下
rec = Rectangle(p_l,p_r)

'''
打开一个URL，获取返回信息
'''
def open_html(real_url):
    NET_STATUS=False
    while not NET_STATUS:
        try:
            data = requests.get(real_url).json()
            return data
        except socket.timeout:
            print("NET_STATURS IS NOT GOOD")
            NET_STATUS=False
        except :
            print("OTHER WRONG")



# 把一个区域的POI点存进数据库
def get_pois(rec,url,data_count):
    url = url.format(lon_l = rec.p_l.lon , lan_l = rec.p_l.lan ,
               lon_r = rec.p_r.lon , lan_r = rec.p_r.lan ,
               types = types,page="{page}")

    f_url = open("url.txt","a",encoding="UTF-8")

    data= open_html(url.format(page=0))

    if data["status"] == "1":
        # 执行到这里，说明url中含有poi数据,下面开始爬取
        all_page=math.ceil(data_count/25)
        for page in range(all_page):
            try:
                # 从第1页开始到第最后一页
                url_real = url.format(page = page+1)
                f_url.write(url_real+'\n')
                data = open_html(url_real)
                if data["status"] == "1" : # 判断第page+1页是否有内容
                    pois = data["pois"]

                    # 将数据储存在数据库
                    # 优化点：db先创建好，一次用完不关，最后再关
                    global db_global
                    db = db_global
                    cursor = db.cursor()
                    str_sql = 'insert into t_poi(id,name,address,typecode,lon,lan,pcode,pname,citycode,cityname,adcode,adname) ' \
                              'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    count=0
                    for poi in pois:
                        try:
                            count += 1
                            value = [poi['id'], poi['name'], poi["address"], poi['typecode'],
                                     poi['location'].split(',')[0], poi['location'].split(',')[1],
                                     poi['pcode'], poi['pname'], poi['citycode'], poi['cityname'], poi['adcode'], poi['adname']]

                            if [] in value:
                                value[value.index([])]=''

                            try:
                                cursor.execute(str_sql,value)
                                db.commit()
                            except:
                                Exception
                                # 打印日志文件
                                f=open("wrong.txt",'a',encoding="UTF-8")
                                time_fomat = '%Y-%m-%d %X'
                                time_current = time.strftime(time_fomat)
                                f.write(time_current+ '  第{page}页,第{index}个出错\n'.format(page=page,index=count))
                        except:
                            pass

                    cursor.close()
            except:
                pass


#检测器
run_count = 0


# 爬取，分析一个区域的POI数量，多于800则将区域四等分，并递归，低于800则进行爬取
def crawl_pois(rec,url):

    # 计算进行了几次分析的计数器，判断程序是否卡死
    global run_count;
    run_count += 1;
    f_count = open("count.txt",'a',encoding="UTF-8");
    f_count.write("这是第{_count}次运行分析一个区域\n".format(_count=run_count))

    # 根据矩形区域的经纬度坐标，拼接url
    url_real = url.format(lon_l=rec.p_l.lon, lan_l=rec.p_l.lan,
                     lon_r=rec.p_r.lon, lan_r=rec.p_r.lan,
                     types=types, page="{page}")

    # 获取url返回的数据
    data = open_html(url_real.format(page=0))

    if data["status"] == "1":
        # 执行到这里，说明url中含有poi数据,下面开始判断区域POI数量
        data_count = int(data['count'])
        if  data_count> 800:
            # 如果区域POI数据大于800则进行四等分
            rec_A = Rectangle(Point(0,0),Point(0,0))
            rec_B = Rectangle(Point(0,0),Point(0,0))
            rec_C = Rectangle(Point(0,0),Point(0,0))
            rec_D = Rectangle(Point(0,0),Point(0,0))

            # 经纬度差
            lon_d = (rec.p_r.lon - rec.p_l.lon)/2
            lan_d = (rec.p_l.lan - rec.p_r.lan)/2

            # 计算每个小矩形的左上点，和右下点
            rec_A.p_l.lon = float(format(rec.p_l.lon,'6f'))
            rec_A.p_l.lan = float(format(rec.p_l.lan,'6f'))
            rec_A.p_r.lon = float(format(rec.p_l.lon + lon_d,'6f'))
            rec_A.p_r.lan = float(format(rec.p_r.lan + lan_d,'6f'))

            rec_B.p_l.lon = float(format(rec.p_l.lon + lon_d,"6f"))
            rec_B.p_l.lan = float(format(rec.p_l.lan,"6f"))
            rec_B.p_r.lon = float(format(rec.p_r.lon,"6f"))
            rec_B.p_r.lan = float(format(rec.p_r.lan + lan_d,"6f"))

            rec_C.p_l.lon = float(format(rec.p_l.lon,"6f"))
            rec_C.p_l.lan = float(format(rec.p_r.lan + lan_d,"6f"))
            rec_C.p_r.lon = float(format(rec.p_l.lon + lon_d,"6f"))
            rec_C.p_r.lan = float(format(rec.p_r.lan,"6f"))

            rec_D.p_l.lon = float(format(rec.p_l.lon + lon_d,"6f"))
            rec_D.p_l.lan = float(format(rec.p_r.lan + lan_d,"6f"))
            rec_D.p_r.lon = float(format(rec.p_r.lon,"6f"))
            rec_D.p_r.lan = float(format(rec.p_r.lan,"6f"))

            recs = []
            recs.append(rec_A)
            recs.append(rec_B)
            recs.append(rec_C)
            recs.append(rec_D)

            # 对四个小矩形分别进行爬取,这里使用递归
            for rec_s in recs :
                # 如果一个区域出现异常，就进行下个区域
                try:
                    crawl_pois(rec_s,url)
                except:
                    pass

        else:
            # 如果该矩形区域poi点的数量少于800就进行爬取
            get_pois(rec,url,data_count)

    else:
        print("出错")

'''
下面是程序的入口
'''
# 创建一个全局的连接
db_global = pymysql.connect("localhost", "root", "111111", "poi")

# 对一个矩形进行爬取分析，这里是对武汉地区的左上角和右下角的经纬度围成的矩形进行爬取
crawl_pois(rec,url)

# 关闭数据库
db_global.close()

