# -*- coding: utf-8 -*-
import logging
import pymysql, time


class ConnetPymysql(object):
	def __init__(self, host, port, user, passwd, db, charset='utf8'):
		"""
		:rtype: object
		"""
		self.conn = pymysql.connect(
			
			host=host,
			port=port,
			user=user,
			passwd=passwd,
			db=db,
			charset=charset)
	
	def get_cursor(self):
		cur = self.conn.cursor()
		return cur
	
	def query(self, sql):
		cursor = self.get_cursor()
		try:
			cursor.execute(sql, None)
			result = cursor.fetchall()
		except Exception as e:
			logging.error("mysql query error: %s", e)
			return None
		finally:
			cursor.close()
		return result
	
	def execute(self, sql, param=None):
		cursor = self.get_cursor()
		try:
			cursor.execute(sql, param)
			self.conn.commit()
			affected_row = cursor.rowcount
		except Exception as e:
			logging.error("mysql execute error: %s", e)
			return 0
		finally:
			cursor.close()
		return affected_row
	
	def executemanys(self, sql, params=None):
		cursor = self.get_cursor()
		try:
			cursor.executemany(sql, params)
			self.conn.commit()
			affected_rows = cursor.rowcount
		except Exception as e:
			logging.error("mysql executemany error: %s", e)
			return 0
		finally:
			cursor.close()
		return affected_rows
	
	def close(self):
		try:
			self.conn.close()
		except:
			pass
	
	def __del__(self):
		self.close()


class ExecuteMysql():
	query_sqldata = "SELECT id,word FROM wordlibrary"
	LOCAL_TIME = time.strftime("%Y%m%d", time.localtime())
	TABLE_NAME = "baidurank_{}".format(LOCAL_TIME)
	INSERT_SQL = "INSERT INTO {}(`site_title`, `rank`, `card_type`, `site_reality_url`, `site_domain`, `bd_crypto_url`,`key_id`) VALUES (%s,%s,%s,%s,%s,%s,%s)".format(
		TABLE_NAME)
	CREAT_SQL = """
	                CREATE TABLE IF NOT EXISTS `{}`(
	                `id` int(11) NOT NULL AUTO_INCREMENT,
	                `key_id` INT(11) NOT NULL,
	                `rank` int(11) NOT NULL DEFAULT 0,
	                `card_type` int(11),
	                `site_reality_url` VARCHAR (255) DEFAULT NULL,
	                `site_domain` VARCHAR (110) DEFAULT NULL,
	                `site_title` varchar(255) DEFAULT NULL,
	                `bd_crypto_url` TEXT DEFAULT NULL,
	                PRIMARY KEY (id),
	                CONSTRAINT fk_key_id FOREIGN KEY (key_id) REFERENCES wordlibrary(id) ON UPDATE CASCADE ON DELETE CASCADE

	            ) ENGINE=InnoDB charset=utf8;
	    """.format(TABLE_NAME)
	mysqlserver = ConnetPymysql('172.16.23.148', 3306, 'root', 'itxiong', 'twlseoanalytics')
	

	
	def get_keydata(self) -> object:
		"""

		:rtype: object
		"""
		query_data = self.mysqlserver.query(self.query_sqldata)
		return query_data
	
	def create_table(self):
		self.mysqlserver.execute(self.CREAT_SQL)

	def inser_sqldata(self,params):
		# params = [('华为- 构建万物互联的智能世界', 1, 1599, 'https://www.huawei.com/', 'www.huawei.com', 'http://www.baidu.com/link?url=Isjg9PUH9qxLttcfGLzOggaH3dSA9v1s7VZFuBwq5bW8SvDpQXFTjTbRfLfBx-NV', 5)]
		self.mysqlserver.executemanys(self.INSERT_SQL, params)

	def get_other_keydata(self, oth_querydata):
		query_data = self.mysqlserver.query(oth_querydata)
		return query_data

if __name__ == "__main__":
	params = [('华为- 构建万物互联的智能世界', 1, 1599, 'https://www.huawei.com/', 'www.huawei.com', 'http://www.baidu.com/link?url=Isjg9PUH9qxLttcfGLzOggaH3dSA9v1s7VZFuBwq5bW8SvDpQXFTjTbRfLfBx-NV', 5)]
	
	# ab = ExecuteMysql().inser_sqldata(params)
	ab = ExecuteMysql().create_table()
	print(ab)
