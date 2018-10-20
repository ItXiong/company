# -*- coding: utf-8 -*-
from baiduserp.executemysql import ExecuteMysql
import re

class OperaionMysqlData:
    get_data = ExecuteMysql()

    def getter_data(self):
        sql_query = """SELECT key_id,GROUP_CONCAT(rank) as rank,GROUP_CONCAT(site_domain) AS site_domain,COUNT(key_id) AS total FROM kz_zp_data.baidurank_20180926
        WHERE id in (SELECT id
        FROM kz_zp_data.baidurank_20180926
        WHERE LOCATE('baidu.com', site_domain) = 0 AND (LOCATE('zhipin.com',site_domain)>0  OR LOCATE('liepin.com', site_domain)>0)
        ) group by key_id;"""
        return self.get_data.get_other_keydata(sql_query)

    def create_key_table(self):
        # 创建表
        self.get_data.CREAT_SQL = """
        CREATE TABLE IF NOT EXISTS `statistic_repetition_0926`(
        id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
        key_id INT(11) NOT NULL COMMENT '关键词ID',
        first_site_domain VARCHAR (255) NOT NULL COMMENT '首个排名的域名',
        description VARCHAR(255) DEFAULT NULL COMMENT '描述'
        ) ENGINE = InnoDB CHARSET=utf8
        """
        self.get_data.create_table()

    def insert_key_data(self,key_data):
        # 插入数据
        self.get_data.INSERT_SQL = """INSERT INTO statistic_repetition_0926(key_id,first_site_domain,description)
        VALUES (%s, %s, %s)"""
        self.get_data.inser_sqldata(key_data)

def process_data(mysql_obj):
    liepin_single =[]
    for x_value in mysql_obj:
        site_domain_split  = x_value[2].split(',')
        set_uniq = sorted(set(site_domain_split),key=site_domain_split.index)
        first_domain = set_uniq[0]
        findall_zhipin = re.findall('zhipin.com', first_domain)
        if len(set_uniq) == 1:
            if not findall_zhipin:
                is_double_single = "且猎聘独有"
            else:
                is_double_single = "且直聘独有"
        else:
            if not findall_zhipin:
                is_double_single = "且猎聘在前面"
            else:
                is_double_single = "且直聘在前面"
        liepin_single.append((x_value[0],first_domain,is_double_single))
    return liepin_single

def execute_data():
    execute_data_obj = OperaionMysqlData()  #创建对象
    execute_data_obj.create_key_table()     #创建存表
    mysql_obj = execute_data_obj.getter_data()
    key_id_list = process_data(mysql_obj)  # 处理数据
    execute_data_obj.insert_key_data(key_id_list)    #插入处理好的数据

if __name__ == "__main__":
    execute_data()

