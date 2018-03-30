# coding:utf-8

import caching

def guild_way(name,type_flag,database):
	key = str(name) + str(type_flag)
	_id = caching.get(key)
	if not _id:
		way_data = database.select('guild_data_way',dict(name = name, type_flag = type_flag ))
		if not way_data:
			_id =database.add('guild_data_way',dict(name = name, type_flag = type_flag))
			if _id:
				caching.set(key,_id)
		else:
			_id = way_data['id']
			caching.set(key,_id)
	return _id
