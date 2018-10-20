# -*- coding: utf-8 -*-

import re,requests,time
from urllib.parse import urlparse
from scrapy.selector import Selector
from baiduserp.executemysql import ExecuteMysql

# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class BaiduserpPipeline(object):
    mysql_server = ExecuteMysql()
    
    def __init__(self):
        self.mysql_server.create_table()
        
    def process_item(self, items, spider):
        html_con = items["bodys"]
        if html_con:
            baiduPcHtmlOriginalResultCardSet = html_con.xpath('//div[contains(@class,"c-container")]')
            insert_sql_data = []
            for baiduPcHtmlOriginalResultCard in baiduPcHtmlOriginalResultCardSet:
                baiduPcHtmlOriginalResultCard = Selector(text=baiduPcHtmlOriginalResultCard.extract())
                OriginalResultRankValue = baiduPcHtmlOriginalResultCard.xpath('//div[contains(@class,"c-container")]/@id').extract_first()
                OriginalResultCardType = baiduPcHtmlOriginalResultCard.xpath( '//div[contains(@class,"c-container")]/@srcid').extract_first()
                OriginalEncryptedUrl = self.cover_parser(baiduPcHtmlOriginalResultCard.extract())
                site_title_list = baiduPcHtmlOriginalResultCard.xpath('//h3/a').extract()
                site_title_list = "".join(site_title_list)
                site_title = re.sub('<[^>]*?>|\r|\n','',site_title_list) if site_title_list else None

                if OriginalEncryptedUrl:
                    webRealUrl = self.parse_by_requests(OriginalEncryptedUrl[0])
                    site_url,site_domain = webRealUrl[0],webRealUrl[1]
                else:
                    site_url, site_domain = None, None
                rank_data = (site_title ,int(OriginalResultRankValue), int(OriginalResultCardType), site_url,site_domain,OriginalEncryptedUrl[0], items['id'])
                if len(rank_data) == 7:
                    insert_sql_data.append(rank_data)

            self.mysql_server.inser_sqldata(insert_sql_data)

            
            
    def cover_parser(self, source):
        """
		提取加密地址
		:param source:
		:return:
		"""
        sec_urls = re.findall(r'<h3.*?>\s*?<a.*?href\s?=\s?"'
                              r'(https?://www.baidu.com/link.*?)"',
                              source, re.S)
        return sec_urls

    def parse_by_requests(self, securl, retries=5):
        """
		使用requests模块进行URL解密
		:param securl: 要解密的URL
		:param retries: 失败重试次数
		:return: 解密成功的URL的域名
		"""
        try:
            securl = securl.replace("http://", "https://")
            res = requests.head(securl, timeout=5).headers
        except requests.RequestException:
            timesleep = 30
            if retries > 0:
                timesleep += 30
                time.sleep(timesleep)
                return self.parse_by_requests(securl, retries - 1)
        else:
            # 这里补充一下，通过get来获取键值的，获取不到的话是返回None的
            # 所以需要做一下判断，是否获取成功
            realUrl = res.get("Location")
            if realUrl:
                host = urlparse(realUrl).netloc
                return realUrl, host
            else:
                return None, None




