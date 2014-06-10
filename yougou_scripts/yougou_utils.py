#!/usr/bin/env python
# -*- coding: utf-8 -*-


allyesdsp_utm_source='AD_kbFCWgsSI'


def check_url(url,orderid,ordervalue):
	#if ordervalue==-1:
	#	return True
	#else:
	#	return False
	url=url.lower()
	if url.find('utm_source')>=0 :
		return True
	else:
		return False

def url_parse_utm(url):
	url=url.lower()
	p1=url.split('?')
	source_res=''
	for pp1 in p1:
		if pp1.find('utm_source')>=0 :
			p2=pp1.split('&')
			for pp2 in p2:
				if pp2.find('utm_source')>=0 and len(pp2.split('='))==2:
					source_res=pp2.split('=')[1]
 
	return 	source_res
 
	
def url_parse_order(url):
	res=''
	url=url.lower()
	p1=url.split('?')	
	for pp1 in p1:
		if pp1.find('&')>=0:
			p2=pp1.split('&')
			for pp2 in p2:
				if len(pp2.split('='))==2:
					(tag,value)=pp2.split('=')
					if tag=='orderId' or tag=='id':
						res=value
	return res

def source_parse(dd_,utm_source):
	res=''
	utm_source=utm_source.lower()
	key=allyesdsp_utm_source
 
	key=key.lower()
	if utm_source==key:
		return 'allyesDSP'
	else:
		return 'unknown'

 
 
