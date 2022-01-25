# 查找武汉市的POI点
import queue
import traceback

import requests
import pymysql
import math

from DBUtils.PooledDB import PooledDB

'''
    武汉市坐标：
        左下角：113.705304,29.974252
        右上角:115.089744,31.366201

    高德：
    多边形搜索API接口，请求方式get
        https://restapi.amap.com/v3/place/polygon?parameters
        
    请求参数：
    polygon:如果多边形为矩形，左上，右下经纬度坐标
        左上角：113.705304,31.366201
        右下角：115.089744,29.974252
        以上为例，polygon = 113.705304,31.366201|115.089744,29.974252
    types=010000:POI数据的类型  
    offset=10，每页返回的数据量
    page=1,页码从0开始，第0页和第1页相同，最大100
    
    补充：接口文档说每次最多1000个数据，实测只有800多数据。
          如果一个矩形区域返回的
          
    返回参数：
    status:状态码
    count:返回数据数量
    pois:poi数据
    
    示例：
    https://restapi.amap.com/v3/place/polygon?polygon=113.705304,31.366201|115.089744,29.974252&key=10518669c9fd0a0532e41189d61e1e9b&extensions=all&types=010000&offset=10&page=80
'''  # 高德地图接口说明

'''
爬虫思路：
//利用递归
    优点：思路清晰
    缺点：堆栈内存占用比较大，耗费资源
    
    //点类，用于存储点的坐标
    public class Point{
        private double lon;  //经度
        private double lat;  //维度
    }
    
    //矩形类,用于存储经纬度坐标
    public class Rectangle{
        private Point p_l;  //矩形左上角的点
        private point p_r;  //矩形右下角的点
    }
    
    //Util类
    public class Util{
          
        /*
            爬取一个矩形区域的POI点
        */
        public crawlPois(Rectangle rec){
            //生成URL并解析
            html = requests.get(url)
            data = html.json()
            
            //判断返回数据是否大于800
            if  int(data['count']) >800:
                //把矩形区域等分成四个
                recs=[]
                recs.append(rec_A)
                recs.append(rec_B)
                recs.append(rec_C)
                recs.append(rec_D)
                
                //对四个小矩形进行递归
                for rec_s in recs:
                    crawlerPois(rec_s)
                
            else:
                //可以获取数据了
                collectionPoi()
        }
    }
     


'''  # 爬取POI点的方法思路

'''
url='https://restapi.amap.com/v3/place/polygon?polygon=113.705304,31.366201|115.089744,29.974252&key=10518669c9fd0a0532e41189d61e1e9b&extensions=all&types=010000&offset=10&page=1'

html = requests.get(url)
data = html.json()

pois = data['pois']
poi = pois[1]

db = pymysql.connect("localhost","root","111111","poi")
cursor = db.cursor()
str_sql = 'insert into t_poi(id,name,address,typecode,lon,lan,pcode,pname,citycode,cityname,adcode,adname) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
value = ((poi['id']),(poi['name']),(poi["address"]),(poi['typecode']),(poi['location'][0:10]),(poi['location'][-9:]),(poi['pcode']),(poi['pname']),(poi['citycode']),(poi['cityname']),(poi['adcode']),(poi['adname']))
cursor.execute(str_sql,value)
db.commit()

cursor.close()
db.close()
'''  # 把数据写入数据库

'''
print("普通连接")
db = pymysql.connect("localhost","root","111111","poi")
cursor = db.cursor()

cursor.execute(str_sql);
results = cursor.fetchall();
if results:
    print(results);
else:
    print("未找到任何数据！！")

cursor.close()
db.close()

str_sql = "select flag from t_rec where id = 2"
print("************************")
print("************************")
print("************************")
print("连接池")

db_pool = PooledDB(pymysql, 1, host='localhost', user='root', port=3306,
                   passwd='111111', db='poi', use_unicode=True)
conn_pool = db_pool.connection()
cursor_pool = conn_pool.cursor()
cursor_pool.execute(str_sql)
result = cursor_pool.fetchone()[0]
print(result)

cursor_pool.close()
conn_pool.close()

'''  # 读取结果集

'''
rec_id = 0

# point
class Point:
    """
    用经纬度来描述一个点
    """

    def __init__(self, lon, lan):
        self.lon = lon  # 经度
        self.lan = lan  # 纬度


class Rectangle:
    """
    用左上角的经纬度和右下角的经纬度描述一个矩形区域
    """
    flag = "False"
    id = 0

    def __init__(self, p_l, p_r):
        self.p_l = p_l  # 左上角的点
        self.p_r = p_r  # 右下角的点

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


# 郑州
rec_zhengzhou = Rectangle(
    Point(112.726469, 34.983807),
    Point(114.244241, 34.271630)
)
'''  # 点，矩形类

# ####################测试出现的问题


rec_id = 0

# point
class Point:
    """
    用经纬度来描述一个点
    """

    def __init__(self, lon, lan):
        self.lon = lon  # 经度
        self.lan = lan  # 纬度


class Rectangle:
    """
    用左上角的经纬度和右下角的经纬度描述一个矩形区域
    """
    flag = "False"
    id = 0

    def __init__(self, p_l, p_r):
        self.p_l = p_l  # 左上角的点
        self.p_r = p_r  # 右下角的点

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


recs = queue.LifoQueue()

# 郑州
rec_zhengzhou = Rectangle(
    Point(112.726469, 34.983807),
    Point(114.244241, 34.271630)
)
temp = rec_zhengzhou.split()
for rec in temp:
    recs.put(rec)
while not recs.empty():
    rec = recs.get()
    print("recs_last.put(Rectange(Point(" + str(rec.p_l.lon) + "," + str(rec.p_l.lan) + "),Point(" + str(
        rec.p_r.lon) + "," + str(rec.p_r.lan) + ")))")

'''
rec_id = 0

# point
class Point:
    """
    用经纬度来描述一个点
    """

    def __init__(self, lon, lan):
        self.lon = lon  # 经度
        self.lan = lan  # 纬度


class Rectangle:
    """
    用左上角的经纬度和右下角的经纬度描述一个矩形区域
    """
    flag = "False"
    id = 0

    def __init__(self, p_l, p_r):
        self.p_l = p_l  # 左上角的点
        self.p_r = p_r  # 右下角的点

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


# 郑州
rec_zhengzhou = Rectangle(
    Point(112.726469, 34.983807),
    Point(114.244241, 34.271630)
)

temp = rec_zhengzhou.split()
recs = queue.LifoQueue()

for rec in temp:
    recs.put(rec)

while not recs.empty():
    rec = recs.get()

    print ("lon_l,lan_l: "+str(rec.p_l.lon) +", "+str(rec.p_l.lan)+"; lon_r,lan_r: "+str(rec.p_r.lon) +", "+str(rec.p_r.lan) )

''' # 队列复制

'''
types = '010000|020000|030000|040000|050000|060000|' \
        '070000|080000|090000|100000|110000|120000|' \
        '130000|140000|150000|160000|170000|180000|' \
        '190000|200000|210000|220000|230000|240000|' \
        '970000|990000'
url='https://restapi.amap.com/v3/place/polygon?polygon=113.705304,31.366201|115.089744,29.974252&key=10518669c9fd0a0532e41189d61e1e9b&extensions=all&types={types}&offset=10&page=30'

data = requests.get(url.format(types=types)).json()
pois = data["pois"]
poi = pois[5]

db = pymysql.connect("localhost","root","111111","poi")
cursor = db.cursor()
str_sql = 'insert into t_poi(id,name,address,typecode,lon,lan,pcode,pname,citycode,cityname,adcode,adname) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
value = [poi['id'], poi['name'], poi["address"], poi['typecode'],
         poi['location'].split(',')[0], poi['location'].split(',')[1],
         poi['pcode'], poi['pname'], poi['citycode'], poi['cityname'], poi['adcode'], poi['adname']]
if [] in value :
    value[value.index([])]=''
cursor.execute(str_sql,value)
db.commit()
cursor.close()
'''  # 如果读取到的POI某个数据为空，则向数据库的对应列中写入空

'''
a=10
b=5
for i in range(10):
    try:
        print(a,b,a/b)
    except Exception as e:
        traceback.print_exc()
    b-=1
'''  # 出现异常则跳过，进行下一次循环

'''
lon_l = 113.705304
lan_l = 31.366201
lon_r = 115.089744
lan_r = 29.974252


# point
class Point:
    """
    用经纬度来描述一个点
    """
    def __init__(self,lon,lan):
        self.lon = lon      # 经度
        self.lan = lan      # 纬度


# area
class Rectangle:
    """
    用左上角的经纬度和右下角的经纬度描述一个矩形区域
    """
    def __init__(self,p_l,p_r):
        self.p_l = p_l      # 左上角的点
        self.p_r = p_r      # 右下角的点


# 初始矩形的左上点和右下点
p_l = Point(lon_l,lan_l)
p_r = Point(lon_r,lan_r)

# 初始矩形，內含两点，左上，右下
rec = Rectangle(p_l,p_r)

# 分割小矩形

count = 0

def split_rec(rec,count):
    count += 1
    if count<=10:
        rec_A = Rectangle(Point, Point)
        rec_B = Rectangle(Point, Point)
        rec_C = Rectangle(Point, Point)
        rec_D = Rectangle(Point, Point)

        # 经纬度差
        lon_d = (rec.p_r.lon - rec.p_l.lon) / 2
        lan_d = (rec.p_l.lan - rec.p_r.lan) / 2

        # 计算每个小矩形的左上点，和右下点
        rec_A.p_l = rec.p_l
        rec_A.p_r.lon = rec.p_l.lon + lon_d
        rec_A.p_r.lan = rec.p_r.lan + lan_d

        rec_B.p_l.lon = rec.p_l.lon + lon_d
        rec_B.p_l.lan = rec.p_l.lan
        rec_B.p_r.lon = rec.p_r.lon
        rec_B.p_r.lan = rec.p_r.lan + lan_d

        rec_C.p_l.lon = rec.p_l.lon
        rec_C.p_l.lan = rec.p_r.lan + lan_d
        rec_C.p_r.lon = rec.p_l.lon + lon_d
        rec_C.p_r.lan = rec.p_r.lan

        rec_D.p_l.lon = rec.p_l.lon + lon_d
        rec_D.p_l.lan = rec.p_r.lan + lan_d
        rec_D.p_r = rec.p_r

        recs = []
        recs.append(rec_A)
        recs.append(rec_B)
        recs.append(rec_C)
        recs.append(rec_D)


        for rec_s in recs:
            print(str(rec_s.p_l.lon)+','+str(rec_s.p_l.lan)+'|'+str(rec_s.p_r.lon)+','+str(rec_s.p_r.lan))
            split_rec(rec_s,count)


split_rec(rec,count)
'''  # 递归分割矩形

'''
conn = pymysql.connect("localhost","root","111111","poi")
cursor = conn.cursor()
str_sql_select = 'select id from t_rec ' \
                 'where ' \
                 'lon_l=%s and lan_l=%s and lon_r=%s and lan_r=%s'

value_select = ['113.705304','31.366201','114.397524','30.670226']
cursor.execute(str_sql_select, value_select)
result = cursor.fetchone()
if result:
    print("已存在")
else:
    print("不存在，请插入")

'''  # sql语句语法错误，在sql字符串拼接时，where后面要有空格
