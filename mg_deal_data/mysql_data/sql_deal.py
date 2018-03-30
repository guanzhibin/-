#coding:utf
import time
class deal_sql(object):
    def inert_sql(self,m_table,params):
        m_table = m_table+'(%s)'
        sql = "INSERT INTO %s values(" % m_table
        keys = []
        values = []
        for k,v in params.items():
            keys.append("`" + k+ "`"),
            values.append(v)
            sql+="'%s',"
        sql = sql[:-1]+')'
        values.insert(0,','.join(keys))
        return sql,tuple(values)
    def inert_sql2(self,m_table,col_name):
        m_table = m_table+'(%s)'
        sql = "INSERT INTO %s values(" % m_table
        key = ','.join(["`" + i + "`" for i in col_name])
        sql = sql % key
        param = ','.join(['%s' for _ in col_name])
        sql = sql + param + ')'
        return sql

    def select_sql(self,m_table,params,offset=None,limit=None,field =None,in_condition=None):
        sql = "select %s from %s"
        if field:
            sql = sql % (','.join(field),m_table)
        else:
            sql = sql % ('*',m_table) 
        values = []
        keys = []
        for k,v in params.items():
            keys.append(k+'=%s')
            values.append(v)
        if in_condition:
            for k2,v2 in in_condition.items():
                keys.append(k2+' in%s')
                values.append(v2)
        if keys:
            sql +=' where '+' and '.join(keys)
        if offset and limit:
            sql+=' limit %s,%s'
            values.append(offset)
            values.append(limit)
        elif limit:
            sql+=' limit %s'
            values.append(limit)

        return sql,tuple(values)
    def update_sql(self,m_table,data,condition):
        ##data,condition均为字典
        sql = "UPDATE %s SET " % m_table
        values = []
        for k,v in data.items():
            sql+=k+'=%s,'
            values.append(v)
        sql = sql[:-1]
        if condition:
            sql += ' where '
            keys_list = []
            for k,v in condition.items():
                keys_list.append(k+'=%s')
                values.append(v)
            sql += ' and '.join(keys_list)
        return sql,tuple(values)

    def delete_sql(self,m_table,params):
        sql = "delete from %s" % m_table
        values = []
        keys = []
        for k,v in params.items():
            keys.append(k+'=%s')
            values.append(v)
        if keys:
            sql +=' where '+' and '.join(keys)

        return sql,tuple(values)