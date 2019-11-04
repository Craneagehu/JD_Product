#-*- coding:utf-8 -*-
import  requests
import pymongo
import urllib3
urllib3.disable_warnings()

class JD():
    def __init__(self):
        self.category_url = 'https://dc.3.cn/category/get'

    def Category(self):
        response = requests.get(self.category_url,verify=False)
        #得到全站数据
        items = response.json()['data']
        #遍历数据列表
        for item in items:
            b_category = item['s'][0]
            #获取大分类信息列表
            b_category_info = b_category['n']
            b_category_name,b_category_url = self.get_category_name_url(b_category_info)

            #获取中分类信息列表
            m_category_s = b_category['s']
            for m_category in m_category_s:
                m_category_info = m_category['n']
                m_category_name, m_category_url = self.get_category_name_url(m_category_info)

                #获取小分类信息列表
                s_category_s = m_category['s']
                for s_category in s_category_s:
                    s_category_info = s_category['n']
                    s_category_name, s_category_url = self.get_category_name_url(s_category_info)
                    self.save_category(b_category_name,b_category_url,m_category_name,m_category_url,s_category_name, s_category_url)

    # 获取分类信息的url和名称
    def get_category_name_url(self,category_info):

        #此分类信息分为三种类型：
            #channel.jd.com / p_guoxueguji.html | 传统文化 | | 0
            # 1713 - 3290 - 6594 | 国家公务员 | | 0,  '-'替换为','
            # 1713 - 3273 | 历史 | | 0


        category = category_info.split('|')
        #分类url
        category_url = category[0]
        #分类名称
        category_name = category[1]

        #第一中分类信息url
        if category_url.count('jd.com') == 1:
            # channel.jd.com/p_guoxueguji.html
            category_url = 'https://'+category_url

        elif category_url.count('-') == 1:
            # 1713 - 3273 | 历史 | | 0
            category_url = 'https://channel.jd.com/{}.html'.format(category_url)

        else:
            # 1713 - 3290 - 6594 | 国家公务员 | | 0
            category_url ='https://list.jd.com/list.html?cat={}'.format(category_url)

        return category_name,category_url

    #保存分类信息
    def save_category(self,b_category_name,b_category_url,m_category_name,m_category_url,s_category_name, s_category_url):
        category_data = {'b_category_name':b_category_name,'b_category_url':b_category_url,'m_category_name':m_category_name,'m_category_url':m_category_url,'s_category_name':s_category_name,'s_category_url':s_category_url}
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        #创建数据库
        mydb = myclient['JD']
        #创建集合
        mycollection = mydb['Category']
        mycollection.insert_one(category_data)

if __name__ =="__main__":
    jd = JD()
    jd.Category()