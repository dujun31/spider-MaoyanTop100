#抓取猫眼电影TOP100榜
from multiprocessing import Pool
from requests.exceptions import RequestException
import requests
import json,csv,time,re

def get_one_page(url):
    '''获取单页源码'''
    try:
        headers = {
            "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        }
        res = requests.get(url, headers=headers)
        time.sleep(1)
        # 判断响应是否成功
        if res.status_code == 200:
            # print(res.text)
            return res.text
        return ('status_code error')
    except RequestException:
        return ('RequestException error')

def parse_one_page(html):
    '''解析单页源码'''
    pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?name"><a.*?>(.*?)</a>.*?star">(.*?)</p>.*?releasetime'
                         + '.*?>(.*?)</p>.*?score.*?integer">(.*?)</i>.*?>(.*?)</i>.*?</dd>',re.S)
    items = re.findall(pattern,html)
    #循环提取信息
    for item in  items:
        yield {
            'rank' :item[0],
            'name':item[1],
            # 'actor':item[2].strip()[3:] if len(item[2])>3 else '',  #判断是否大于3个字符
            # 'time' :item[3].strip()[5:] if len(item[3])>5 else '',
            'actor':item[2].strip()[3:],
            'time' :item[3].strip()[5:15],
            'score':item[4] + item[5]
        }

def write_to_textfile(content):
    '''写入text'''
    with open("MovieResult.text",'a',encoding='utf-8') as f:
        #利用json.dumps()将字典序列化,并将ensure_ascii设置为False,从而显示中文.+换行
        f.write(json.dumps(content,ensure_ascii=False) + "\n")
        f.close()

def write_to_csvField(fieldnames):
    '''写入csv表头'''
    with open("MovieResult.csv", 'a', encoding='gb18030', newline='') as f:
        #将字段名传给Dictwriter来初始化一个字典写入对象
        writer = csv.DictWriter(f,fieldnames=fieldnames)
        #调用writeheader方法写入字段名
        writer.writeheader()

def write_to_csvRows(content,fieldnames):
    '''写入csv内容'''
    with open("MovieResult.csv",'a',encoding='gb18030',newline='') as f:
        #将字段名传给Dictwriter来初始化一个字典写入对象
        writer = csv.DictWriter(f,fieldnames=fieldnames)
        writer.writerows(content)
        f.close()

# 将字段名传入列表
fieldnames = ["rank", "name", "actor", "time", "score"]

def task(offset):
    url = "http://maoyan.com/board/4?offset={0}".format(offset)
    html = get_one_page(url)
    rows = []
    for item in parse_one_page(html):
        # write_to_textfile(item)
        rows.append(item)
    # 写入csv内容
    write_to_csvRows(rows,fieldnames)

if __name__ == '__main__':
    #写入csv表头
    write_to_csvField(fieldnames)
    #map方法会把每个元素当做函数的参数,,在进程池中创建多进程.循环写入10页传参
    pool = Pool()
    pool.map(task,[i*10 for i in range(10)])