#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib, urllib2
import json

import sys
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
    
    
class GeoCoding(object):
    def __init__(self, key = ' '):
        self.url_para = {'address': '', 
                 'sensor': 'false',
                 'language': 'zh-CN'}
        self.url = 'http://maps.googleapis.com/maps/api/geocode/json'
        self.geo_info_list = []
        
    def get_latlng_by_name(self, geo_name):
        self.url_para['address'] = geo_name.encode('utf-8')
        arguments = urllib.urlencode(self.url_para)
        url_get_geo = self.url + '?' + arguments
        handler = urllib2.urlopen(url_get_geo)
        resp_data = handler.read()
        handler.close()
        st = self.parse_ret_json(resp_data)
        return self.geo_info_list

    def parse_ret_json(self, ret_str):
        parse_st = False
        ret_json = json.loads(ret_str)
        if ret_json['status'] == 'OK':
            #get lat lng and addr
            for geo_info in ret_json['results']:
                #print(geo_info)
                geo_dict = {'lat': geo_info['geometry']['location']['lat'],
                            'lng': geo_info['geometry']['location']['lng'],
                            'addr': geo_info['formatted_address'],
                            'city':'',
                            'state_province':'',
                            'country':'',
                            'types': geo_info['types']}
                #get city state_provine country
                for addr_comp in geo_info['address_components']:
                    if 'country' in addr_comp['types']: 
                        geo_dict['country'] = addr_comp['long_name']
                    elif 'administrative_area_level_1' in addr_comp['types']:
                        geo_dict['state_province'] = addr_comp['long_name']
                    elif 'sublocality' in addr_comp['types'] or \
                         'locality' in addr_comp['types'] or \
                         'administrative_area_level_2' in addr_comp['types'] or \
                         'administrative_area_level_3' in addr_comp['types']:
                        geo_dict['city'] = addr_comp['long_name']
                self.geo_info_list.append(geo_dict) 
            parse_st = True
            #print(self.geo_info_list)
        else:
            parse_st = False 
            print(ret_json['status']) 
        return parse_st


res=[]
f=open('region.txt')
for line in f.readlines():
	line=line.strip('\n')
	(city,imp,clk,ctr)=line.split('\t')
	g=GeoCoding()
	js=GeoCoding.get_latlng_by_name(g,city)
	try:
		lat=str(js[0]['lat'])
		lng=str(js[0]['lng'])
		tmp={}
		tmp['city']=city
		tmp['pos']=[float(lng),float(lat)]
		tmp['imp']=int(imp)
		tmp['clk']=int(clk)
		tmp['ctr']=float(ctr)
		res.append(tmp) 
	
		
	except:
		print city,js

g=open('regionjs.json','w')
g.write(json.dumps(res, encoding='UTF-8', ensure_ascii=False))


 
 
 
 
 
 
 








