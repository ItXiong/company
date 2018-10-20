# -*- coding: utf-8 -*-
import scrapy,re
from baiduserp.items import BaiduserpItem
from baiduserp.executemysql import ExecuteMysql

class BaiduSerp(scrapy.Spider):
	name = "baiduserp"

	def __init__(self,*args, **kwargs):
		super().__init__(*args, **kwargs)
		# ExecuteMysql().create_table()
		self.read_sqldata = ExecuteMysql().get_keydata()
	
	def start_requests(self):
		for urls in self.read_sqldata:
			url = "http://www.baidu.com/s?wd={0}&rn=10".format(urls[1])
			yield scrapy.Request(url=url, meta={'item': urls[0],'title':urls[1]},callback=self.parse)
			
	def parse(self, response):
		item = BaiduserpItem()
		item['id'] = response.meta['item']
		item['bodys'] = response
		return item
	