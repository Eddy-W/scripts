#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
 
import os
import sys


from xlrd import open_workbook  
from xlutils.copy import copy
from xlwt import Workbook
from xlwt import Style, XFStyle, Borders, Pattern, Font ,Alignment
from xlwt import easyxf
from xlutils.styles import Styles

import urllib2,re,string
from time import sleep
 
import json



fp=sys.argv[1]+'/'
dt=sys.argv[2]
dd=sys.argv[3]


import time

ts=time.time()*pow(10,6)

url="http://adm.yihaodian.com/adoutside/service/analysis/getOrderInfo.action?params=%s_20_allYesDsp@yhd_0_06b8fd4570c747741b7dfee434e9dd_%d"%(dd,ts)

#Track log : http://adm.yihaodian.com/adoutside/service/analysis/getTrackInfo.action?params=2014-01-27_20_allYesDsp@yhd_06b8fd4570c747741b7dfee434e9dd_139582739391000
#Order list : http://adm.yihaodian.com/adoutside/service/analysis/getOrderInfo.action?params=2014-01-27_20_allYesDsp@yhd_0_06b8fd4570c747741b7dfee434e9dd_1395729045000
#Product list of Order : http://adm.yihaodian.com/adoutside/service/analysis/getOrderProductInfo.action?params=2014-03-25_20_allYesDsp@yhd_0_06b8fd4570c747741b7dfee434e9dd_139582739391000

statusdic={}
statusdic["0"]=["未知","No"]
statusdic["20"]=["已出库","Yes"]
statusdic["24"]=["货物用户已收到(或退换货完成)","Yes"]
statusdic["3"]=["已下单(货款未全收)","No"]
statusdic["34"]=["订单取消(失败)","Yes"]
statusdic["38"]=["已转DO","Yes"]
statusdic["4"]=["已下单(货款已收)","No"]
statusdic["37"]=["送货失败(其它)","Yes"]	





def downloadfile(file_url,filename,download_path):
	try:
		request = urllib2.Request(file_url) 
		f=open(download_path+filename,'wb')
		 
		start_time=time.time()
		#print 'time stamp is : ',time.time()
		print start_time 
		size =0
		speed=0
		data_lines = urllib2.urlopen(request).readlines()
		#data = urllib2.urlopen(request).read() 
		for data in data_lines:
			f.write(data)
			size = size + len(data)
			dural_time=float(time.time()) - float(start_time)
			if(dural_time>0):
				speed = float(size)/float(dural_time)/(1000*1000)
				while(speed >1):
					print 'speed lagger than 1MB/s , sleep(0.1).....'
					print 'sleep .....'
					sleep(0.1)
					dural_time=float(time.time()) - float(start_time)
					speed = float(size)/float(dural_time)/(1000*1000)
		print 'total time is : ',dural_time ,'seconds'
		print 'size is       : ',size ,'KB'
		print 'speed is      : ',speed ,'MB/s'  
	except Exception,e:
		print 'download error: ',e
		return False
	return True

fxml="orderstate_%s.xml"%time.strftime('%m%d%H%M',time.localtime(time.time()))
#fxml="orderstate_test.xml"
downloadfile(url,fxml,fp)

from xml.etree import ElementTree as ET
ff=fp+fxml
per=ET.parse(ff)

orders=per.findall('/order-info')
orderdic={}
for order in orders:
	#print order.find("order-id").text,order.find("order-status").text
	orderdic[order.find("order-id").text]=order.find("order-status").text
for k,v in orderdic.items():
	print k,v
	
fin=open(fp+"output-2")
fout=open(fp+"output-3",'wb')
for line in fin.readlines():
	line=line[:-1].replace('\x01','\t')
	line=line.replace('\\N','NULL')

	(sid,sname,bid,bname,order_ts,orderid,ordervalue)=line.split('\t')
	status="0"
	if orderdic.has_key(orderid):
		status=orderdic[orderid]
	str="%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(sid,sname,bid,bname,order_ts,orderid,ordervalue,status,statusdic[status][0],statusdic[status][1])
	fout.write(str+'\n')
fin.close()
fout.close()












