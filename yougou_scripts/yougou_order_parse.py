#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys, os
sys.path.append(os.getcwd())
import yougou_utils as yougou
orderid_dict={}
 
def fill_data(lines):
	 
	if len(lines)==0:
		return
	 
	order_id=''
	order_value=''
	order_source=''
	utm_source=''
	landing_ts=''
	order_ts=''
	landing_ref=''
	landing_url=''
	for line in lines:
 
		(ts_,cookie_,url_,ref_,orderid_,ordervalue_,dd_) = line
		
		if yougou.check_url(url_,orderid_,ordervalue_):
 			utm_source=yougou.url_parse_utm(url_)	
			landing_ts=ts_
			landing_ref=ref_
			landing_url=url_
			order_source=yougou.source_parse(dd_,utm_source)
			#sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (ts_,cookie_,url_,orderid_,landing_ts,'','','',order_source,utm_source))
		 
		else:
			order_id=orderid_
			order_value=ordervalue_
			if orderid_dict.has_key(order_id)!=True:
				order_ts=ts_
				if order_source!='unknown' and order_source!='':
					sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (ts_,cookie_,landing_url,landing_ref,orderid_,landing_ts,order_ts,order_id,order_value,order_source,utm_source))
				orderid_dict[order_id]=order_source
				order_id=''
				order_source=''		
				utm_source=''
				order_value='' 
				landing_ts=''
				order_ts=''
				landing_ref=''
				landing_url=''
 

last_key, lines = None, []
for line in sys.stdin:
	try:
		tpl = (ts,cookie,url,ref,orderid,ordervalue,dd) = line.strip('\n').split('\t')
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

 
