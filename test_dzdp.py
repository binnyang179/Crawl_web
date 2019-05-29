from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ChromeOptions

from pyquery import PyQuery as pq
import requests
import pymongo
import pymysql
from time import sleep
from lxml import etree
import os
from io import StringIO

option = ChromeOptions()
option.add_experimental_option('excludeSwitches', ['enable-automation'])
browser = webdriver.Chrome(options=option)


URL_TO_CRAWL = 'https://www.dianping.com/'
KEYWORD = '火锅'

ALL_IN_ONE = []
MAX_PAGE = 50

MONGO_URL = 'localhost'
MONGO_DB = 'dzdp'
MONGO_COLLECTION = 'items'
client = pymongo.MongoClient(MONGO_URL)
db_for_mongo = client[MONGO_DB]

db_for_mysql = pymysql.connect(host='localhost', user='root', password='123456')
cursor = db_for_mysql.cursor()


def get_judge():
    init_mysql_for_judge()

    # url = 'http://www.dianping.com/shop/97404408'
    # headers = {
    #     'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
    #     'Host': 'httpbin.org'
    # }
    # req = urllib.request.get(url=url,  headers=headers)
    # response = urllib.request.urlopen(req)
    # ahtml=response.read().decode('utf-8')
    # ahtml = req
    # print(ahtml)

    # asf = etree.parse('ass.html', etree.HTMLParser)

    html = browser.page_source
    with open('as.html','w') as f:
        f.write(html)
    doc = pq(html)
    judegs_items = doc('#reviewlist-wrapper').find('.content').items()
    for judegs_item in judegs_items:
        print(judegs_item)
        judge = judegs_item.find('.desc').text()
        print(judge)
        judge_images = judegs_item.find('.photos').find('.item .J-photo').items()
        for judge_image in judge_images:
            print(judge_image)
            image_judge_url = judge_image.find('img').attr('src')
            judge_info = {
                'judge': judge,
                'judge_img': image_judge_url
            }
            save_to_mongo(judge_info)
            save_judge_for_mysql(judge_info)
            save_image_to_folder(image_judge_url, str(judge)[1:6])


def get_stores():
    init_mysql_for_store()

    html = browser.page_source
    doc = pq(html)
    items_pic = doc('.shop-all-list .pic').items()
    for item in items_pic:
        aa = ''.join(str(item))
        asd = etree.parse(StringIO(aa), etree.HTMLParser())
        agc = etree.parse(StringIO(aa), etree.HTMLParser())
        img_url = str(asd.xpath('//img/@src'))[2:-2]
        store = str(agc.xpath('//img/@title'))[2:-2]
        store_info = {
            'store': store,
            'image_url': img_url
        }
        save_to_mongo(store_info)
        save_store_to_mysql(store_info)
        save_image_to_folder(str(store_info['image_url'])[:-1], str(store_info['store'])[1:6])


def save_image_to_folder(img_url, img_name):
    print('begin to save to folder')
    print(img_url,',',img_name)
    if img_url[6:]:
        r = requests.get(img_url)
        with open('./images_dzdp/'+ img_name+ '.jpg','wb') as f:
            f.write(r.content)
    print('success to save to folder')


def save_to_mongo(product):
    try:
        if db_for_mongo[MONGO_COLLECTION].insert(product):
            print('存储到MongoDB成功')
    except Exception:
        print('存储到MongoDB失败')




def init_mysql_for_item():
    print(' begin to init mysql')
    sql_drop_db = 'drop database if exists exam;'
    sql_create_db = 'create database if not exists exam;'
    sql_drop_table = 'drop table if exists exam.dzdpitem;'
    sql_create_table = 'create table if not exists exam.dzdpitem(name varchar(300));'
    cursor.execute(sql_drop_db)
    cursor.execute(sql_create_db)
    cursor.execute(sql_drop_table)
    cursor.execute(sql_create_table)
    print(' init success')


def init_mysql_for_store():
    print(' begin store info')
    sql_create_db = 'create database if not exists exam;'
    sql_drop_table = 'drop table if exists exam.dzdpstore;'
    sql_create_table = 'create table if not exists exam.dzdpstore(name varchar(50), url varchar(400));'
    cursor.execute(sql_create_db)
    cursor.execute(sql_drop_table)
    cursor.execute(sql_create_table)
    print('init for store success')


def init_mysql_for_judge():
    print(' begin judge info')
    sql_create_db = 'create database if not exists exam;'
    sql_drop_table = 'drop table if exists exam.dzdpjudge;'
    sql_create_table = 'create table if not exists exam.dzdpjudge(judge varchar(240), url varchar(300));'
    cursor.execute(sql_create_db)
    cursor.execute(sql_drop_table)
    cursor.execute(sql_create_table)
    print('init for store success')


def save_judge_for_mysql(product):
    print('begin judge')
    sql = 'INSERT INTO exam.dzdpjudge values(%s, %s);'
    a = str(product['judge'])
    b = str(product['judge_img'])
    print(sql,(a,b))
    try:
        cursor.execute(sql,(a, b))
        db_for_mysql.commit()
        print(' success save to mysql')
    except:
        db_for_mysql.rollback()
        print(' failed to save to mysql ')


def save_item_to_mysql(product):
    print(' begin to save to mysql')
    sql = 'INSERT INTO exam.dzdpitem values(%s);'
    try:
        cursor.execute(sql,(product))
        db_for_mysql.commit()
        print(' success save to mysql')
    except:
        db_for_mysql.rollback()
        print(' failed to save to mysql ')


def save_store_to_mysql(product):
    print('begin store mysql')
    sql = 'INSERT INTO exam.dzdpstore values(%s, %s);'
    a = str(product['store'])
    b = str(product['image_url'])
    print(sql,(a,b))
    try:
        cursor.execute(sql,(a, b))
        db_for_mysql.commit()
        print(' success save to mysql')
    except:
        db_for_mysql.rollback()
        print(' failed to save to mysql ')


def get_all_items():

    init_mysql_for_item()
    browser.get(URL_TO_CRAWL)
    html = browser.page_source


    doc = pq(html)

    items = doc('.first-item').items()

    for item in items:

        doc_item = pq(item)
        TITLE_EVERYTHING = []

        items_show = doc_item('.primary-container').text()
        TITLE_EVERY = items_show.split('\n')
        aa = ''.join(TITLE_EVERY)
        save_item_to_mysql(aa)
        item_for_mongo = {
            'name': aa
        }
        save_to_mongo(item_for_mongo)

        TITLE_EVERYTHING.append(TITLE_EVERY)

        items_hide = doc_item('.groups').items()

        for item_hide in items_hide:

            doc_a = pq(item_hide)
            littles_item = doc_a('.group').items()
            for little in littles_item:
                each_group = []
                each_item_title_for_append = little.find('.channel-title').text()
                each_group.append(each_item_title_for_append)

                doc_item_each = pq(little)
                item_little_contents = doc_item_each('.second-item').items()
                for item_little_content in item_little_contents:
                    item_content_lit = item_little_content.text()
                    each_group.append(item_content_lit)
                bb = ''.join(each_group)
                save_item_to_mysql(bb)
                item_for_mongo = {
                    'name': bb
                }
                save_to_mongo(item_for_mongo)

                TITLE_EVERYTHING.append(each_group)
        ALL_IN_ONE.append(TITLE_EVERYTHING)


def main():
    if os.path.exists('images_dzdp') is False:
        os.mkdir('images_dzdp')
    # get_all_items()
    print(ALL_IN_ONE)

    browser.get(URL_TO_CRAWL)
    jump_to_hotpot = browser.find_element_by_link_text(KEYWORD)
    changed_url = jump_to_hotpot.get_attribute('href')
    browser.get(changed_url)
    start_url = browser.current_url
    print('current url is ' , start_url)

    sleep(2)
    for i in range(1, 2):
        print('di ', i)
        # get_stores()
        print(' di ', i, ' page is done')
        html = browser.page_source
        jump_to_next = browser.find_element_by_class_name('next')
        jump_to_next.click()


    jump = browser.find_element_by_xpath('//a[@data-shopid="98795655"]')
    change = jump.get_attribute('href')
    print(jump)
    browser.quit()
    browser.get(change)
    # jsb = "window.scrollTo(0, document.body.scrollHeight-2500)"
    # browser.execute_script(jsb)
    sleep(1)
    get_judge()


if __name__ == '__main__':
    main()
