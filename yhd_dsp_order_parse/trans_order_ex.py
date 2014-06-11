#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys, os


orderid_dict={}
 
def fill_data(lines):
	 
	if len(lines)==0:
		return
	 
	order_id=''
	order_value=''
	order_bid=''
	order_sid=''
	order_ts=''
	for line in lines:
 
		(cookie,ts,bid,sid,orderid,ordervalue,ref) = line
		
		if bid!='' and sid!='':
			order_bid=bid
			order_sid=sid
			order_ts=ts
			#sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (ts_,cookie_,url_,orderid_,landing_ts,'','','',order_source,utm_source))
		 
		elif orderid!='' and ordervalue!='':
			order_id=orderid
			order_value=ordervalue
			if orderid_dict.has_key(order_id)!=True:
				order_ts=ts	
			 
				sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (cookie,order_ts,order_bid,order_sid,orderid,ordervalue,ref))
				orderid_dict[order_id]=order_value
	
 

last_key, lines = None, []
for line in sys.stdin:
	try:
		tpl = (cookie,ts,bid,sid,orderid,ordervalue,ref) = line.strip('\n').split('\t')
		key = cookie
		if last_key != key and last_key is not None:
			fill_data(lines)
			lines = []
		last_key = key
		lines.append(tpl)
	except:
		print line
		#pass

fill_data(lines)
