#-*- coding:utf-8 -*-
import time
import pymongo
import requests
import urllib3
urllib3.disable_warnings()
from lxml import etree
from jsonpath import jsonpath
from urllib.parse import urljoin
from multiprocessing.dummy import Pool
#防止连接过多，出现异常
requests.adapters.DEFAULT_RETRIES = 5  #重新连接
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

class JDProductSpider(object):
    def __init__(self):
        self.myclient = pymongo.MongoClient('localhost', 27017)
        self.headers = {'Connection': 'close'}

    def get_skuid(self,category_data):
        # category_data = {
        #     'b_category_name': '家用电器', 'b_category_url': 'https://jiadian.jd.com',
        #     'm_category_name': '电视', 'm_category_url': 'https://list.jd.com/list.html?cat=737,794,798',
        #     's_category_name': '超薄电视',
        #     's_category_url': 'https://list.jd.com/list.html?cat=737,794,798&ev=4155_76344&sort=sort_rank_asc&trans=1&JL=2_1_0#J_crumbsBar'
        # }
        page_url = category_data['s_category_url']

        while True:
            response = requests.get(page_url,verify=False,headers=self.headers)
            e = etree.HTML(response.text)

            #获取商品列表sku_id
            sku_ids = e.xpath('//div[contains(@class,"j-sku-item")]/@data-sku')
            for sku_id in sku_ids:
                item = {}
                item['product_category'] = category_data
                item['product_sku_id'] = sku_id
                self.parse_product(sku_id,item)

            # 获取下一页URL,如果是最后一页，循环结束
            next_url = e.xpath('//a[@class="pn-next"]/@href')
            if next_url:
                page_url = urljoin(response.url,next_url[0])

            else:
                break



    def parse_product(self,sku_id,item):
        try:
            # 构建商品基本信息请求，使用app抓包工具对商品基本信息进行抓取
            product_url = f"https://cdnware.m.jd.com/c1/skuDetail/apple/7.3.0/{sku_id}.json"
            resp = requests.get(product_url,verify=False,headers=self.headers)
            result = resp.json()
            #提取数据
            # 1.商品名称
            item['product_name'] = result['wareInfo']['basicInfo']['name']

            # 2.商品图片url(列表中第一张图片)
            item['product_image_url'] = jsonpath(result,'$..small')[0]

            # 3.是否有图书信息
            item['product_book_info'] = jsonpath(result,'$..bookInfo')[0]

            # 4.商品选项
            colorsize = jsonpath(result,'$..colorSize')
            #判断是否商品包含选项
            if colorsize:
                colorsize = colorsize[0]
                product_option = {}
                #遍历商品多个选项，如颜色、版本、款式、尺寸等等
                for option in colorsize:
                    #title为可选项名称
                    title = jsonpath(option,'$..title')[0]
                    #text为可选内容
                    text = jsonpath(option,'$..text')
                    product_option[title] = text
                item['product_option'] = product_option

            # 5.商品店铺
                shop = jsonpath(result,'$..shop')
                #判断是否有店铺信息，京东自营是没有店铺信息的,也可能出现有shop ,但是没有值的(如: "shopInfo":{"shop":null,"customerService":{"hasChat":true,"hasJimi":false,"allGoodJumpUrl":"openApp.jdMobile://virtual?params=)
                if shop:
                    shop = shop[0]
                    if shop:
                        item['product_shop'] = {
                            'shop_id':shop['shopId'],    #店铺shopId
                            'shop_name':shop['name'],
                            'shop_score':shop['score']
                        }
                    else:
                        item['product_shop'] ={'shop_name': '京东自营'}

            # 6.商品类别ID
            product_category_id = result['wareInfo']['basicInfo']['category'].replace(';',',')
            item['product_category_id'] = product_category_id

            #构建促销商品信息的url,经过精简Url,最后只剩下三个参数，skuId：sku_id,area:固定值，cat: 商品分类ID
            #使用F12对商品详情页进行检查，并在search框中搜索促销关键字，便可以得到商品促销信息的json数据，提取url即可
            ad_url = f'https://cd.jd.com/promotion/v2?skuId={sku_id}&area=4_48205_48332_0&cat={product_category_id}'
            resp_ad = requests.get(ad_url,verify=False,headers=self.headers)
            self.parse_product_ad(resp_ad,item)
        #可能出现的异常：访问太快;sku_id的商品没有商品信息
        except Exception as e:
            print(f'出现异常的sku_id: {sku_id}')
            print(f'异常值: {e}')

    def parse_product_ad(self,resp_ad,item):
        #使用apparent_encoding自动解码
        resp_ad.encoding = resp_ad.apparent_encoding
        product_ad = jsonpath(resp_ad.json(),'$..ads')  #jsonpath这是个列表数据,再加上自带列表数据，所以最后是双重列表数据
        #如果没有促销信息，product_ad 返回[None]
        if product_ad[0]:
            item['product_ad'] = product_ad[0][0]['ad']

        else:
            item['product_ad'] = ''

        #商品评论信息
        comments_url = f'https://club.jd.com/comment/productCommentSummaries.action?referenceIds={item["product_sku_id"]}'
        resp_comments = requests.get(comments_url,verify=False,headers=self.headers)
        self.parse_product_comments(resp_comments,item)

    def parse_product_comments(self,resp_comments,item):
        result = resp_comments.json()
        product_comments = {}

        #评论数量
        product_comments['CommentCount'] = jsonpath(result,'$..CommentCount')[0]

        #好评数量
        product_comments['GoodCount'] = jsonpath(result, '$..GoodCount')[0]

        #差评数量
        product_comments['PoorCount'] = jsonpath(result, '$..PoorCount')[0]

        #好评率
        product_comments['GoodRate'] = jsonpath(result, '$..GoodRate')[0]

        item['product_comments'] = product_comments

        #构建价格请求
        price_url = f'https://p.3.cn/prices/mgets?skuIds=J_{item["product_sku_id"]}'


        resp_price = requests.get(price_url,verify=False,headers=self.headers)
        self.parse_product_price(resp_price,item)

    def parse_product_price(self,resp_price,item):
        result = resp_price.json()
        product_price = result[0]['p']
        item['product_price'] = product_price
        self.save_product(item)

    def save_product(self,item):
        mydb = self.myclient['JD']
        # mycollection = mydb['Product']
        mycollection = mydb['Product_air_conditioner']
        mycollection.insert_one(item)
        print(f'{item["product_sku_id"]}插入成功')

    #线程池抓取
    def ThreadPool(self):
        pool = Pool()
        category_data = self.myclient.JD.Category.find( { "m_category_name": { "$regex": '空调', "$options": 'i'}},{"_id":0})
        pool.map(self.get_skuid,category_data)
        pool.close()
        pool.join()


if __name__ == '__main__':
    t1 = time.time()
    jdproduct = JDProductSpider()
    jdproduct.ThreadPool()
    t2 = time.time()
    print(f"耗时: {t2-t1}")