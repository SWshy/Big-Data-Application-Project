import requests
import csv
import time
import random
import json
from bs4 import BeautifulSoup
import re


def get_movie_type_id(url):
    # 设置请求头信息
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    """
    爬取电影类型、类型id
    """
    # 随机等待一段时间，避免被封 IP
    time.sleep(random.randint(1, 5))
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 类型
    types = soup.select('#content > div > div.aside > div:nth-child(1) > div')[0].find_all('a')
    result = []

    for item in types:
        type_id = re.search('type=(\d+)', str(item)).group(1)
        result.append(type_id)

    return result


def get_movie_url(url):
    # 设置请求头信息，模拟浏览器的行为
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # 发送HTTP请求，并获取响应
    response = requests.get(url, headers=headers)

    #每个类型下的url列表
    url_list = []

    # 如果请求成功
    if response.status_code == 200:
        # 将响应内容解析成Python对象
        data = json.loads(response.text)

        #获取电影url
        for item in data:
            url_list.append(item['url'])

    else:
        # 如果请求失败，输出错误信息
        print('Failed to retrieve data')

    return url_list


def get_movie_info(url):
    # 设置请求头信息
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    """
    爬取电影详情页信息
    """
    # 随机等待一段时间，避免被封 IP
    time.sleep(random.randint(1, 5))
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 电影名称
    name = soup.select('#content > h1 > span:nth-child(1)')[0].get_text()

    # 评分
    rating_num = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.rating_self.clearfix > strong')[0].get_text()

    # 评价人数
    rating_count = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.rating_self.clearfix > div > div.rating_sum > a > span')[0].get_text()

    #短评数量
    shortcomment_count = soup.select('#comments-section > div.mod-hd > h2 > span > a')[0].get_text().strip().replace('全部', '').replace('条', '')

    #影评数量
    longcomment_count =soup.select('#reviews-wrapper > header > h2 > span > a')[0].get_text().strip().replace('全部', '').replace('条', '')

    # 又名
    alias = soup.find('span', 'pl', text='又名:').next_sibling.strip()

    # 片长
    length = soup.find(name='span', attrs={'property': 'v:runtime'}).get_text().replace('分钟', '')

    # 语言
    language = soup.find('span', class_='pl', text='语言:').next_sibling.strip()

    # 类型
    genres = [genre.get_text() for genre in soup.find_all(name='span',  attrs={'property': 'v:genre'})]

    # 主演
    actors = [actor.get_text() for actor in soup.find('span', 'actor').find_all('a')]

    # 编剧
    writers = [writer.get_text() for writer in soup.find('span', 'pl', text='编剧').next_sibling.find_next().find_all('a')]

    # 导演
    directors = [director.get_text() for director in soup.find('span', 'pl', text='导演').next_sibling.find_next().find_all('a')]

    # 制片国家/地区
    country = soup.find('span', class_='pl', text='制片国家/地区:').next_sibling.strip()

    # 上映日期
    release_date = [release_dates.get_text() for release_dates in soup.find_all(name='span',  attrs={'property': 'v:initialReleaseDate'})]

    #看过人数
    seen_count = soup.select('#subject-others-interests > div > a:nth-child(1)')[0].get_text().strip().replace('人看过', '')

    #想看人数
    wanne_count = soup.select('#subject-others-interests > div > a:nth-child(2)')[0].get_text().strip().replace('人想看', '')

    # 星级
    stars = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.rating_self.clearfix > div > div:nth-child(1)')[0].get('class')[2].replace('bigstar', '')

    # 评分分布
    star_5 = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.ratings-on-weight > div:nth-child(1) > span.rating_per')[0].get_text().replace('%', '')
    star_4 = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.ratings-on-weight > div:nth-child(2) > span.rating_per')[0].get_text().replace('%', '')
    star_3 = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.ratings-on-weight > div:nth-child(3) > span.rating_per')[0].get_text().replace('%', '')
    star_2 = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.ratings-on-weight > div:nth-child(4) > span.rating_per')[0].get_text().replace('%', '')
    star_1 = soup.select('#interest_sectl > div.rating_wrap.clearbox > div.ratings-on-weight > div:nth-child(5) > span.rating_per')[0].get_text().replace('%', '')

    # 构建字典
    movie_dict = {
        '电影名称': name,
        '评分': rating_num,
        '评价人数': rating_count,
        '短评数量': shortcomment_count,
        '影评数量': longcomment_count,
        '又名': alias,
        '片长': length,
        '语言': language,
        '类型': genres,
        '主演': actors,
        '编剧': writers,
        '导演': directors,
        '制片国家/地区': country,
        '上映日期': release_date,
        '看过人数': seen_count,
        '想看人数': wanne_count,
        '星级': stars,
        '五星': star_5,
        '四星': star_4,
        '三星': star_3,
        '二星': star_2,
        '一星': star_1
    }

    return movie_dict


def save_header():
    """
        创建 CSV 文件并写入表头
    """
    with open('movie.csv', mode='w', encoding='utf-8-sig', newline='') as f:
        fieldnames = [
            '电影名称',
            '评分',
            '评价人数',
            '短评数量',
            '影评数量',
            '又名',
            '片长',
            '语言',
            '类型',
            '主演',
            '编剧',
            '导演',
            '制片国家/地区',
            '上映日期',
            '看过人数',
            '想看人数',
            '星级',
            '五星',
            '四星',
            '三星',
            '二星',
            '一星'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

def save_to_csv(movie_dict):
    """
    保存数据到CSV文件
    """
    with open('movie.csv', 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=movie_dict.keys())
        writer.writerow(movie_dict)


if __name__ == '__main__':
    #获取电影类型id
    url = 'https://movie.douban.com/chart'
    type_id = get_movie_type_id(url)
    print("成功获取电影类型id！")

    # 创建 CSV 文件并写入表头
    save_header()

    # 爬取每个电影的信息并写入 CSV 文件
    for type_id in type_id:
        for i in range(0, 100, 10):
            type_url = f'https://movie.douban.com/j/chart/top_list?type={type_id}&interval_id={i + 10}:{i}&action=&start=0&limit=1000'
            urls = get_movie_url(type_url)
            for url in urls:
                try:
                    movie_dict = get_movie_info(url)
                    save_to_csv(movie_dict)
                except:
                    pass
    print('电影信息已保存到 movies.csv 文件中！')