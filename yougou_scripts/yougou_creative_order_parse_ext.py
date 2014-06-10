#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys, os
 
 
def fill_data(lines): 
     if len(lines)==0:
          return
     ( bid,sid,orderid,orderts,ordervalue,tt,impid,cookie) = lines[0]
     sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (bid,sid,orderid,orderts,ordervalue,impid,cookie))     

last_key, lines = None, []
for line in sys.stdin:
	try:
		tpl = ( bid,sid,orderid,orderts,ordervalue,tt,impid,cookie) = line.strip('\n').split('\t')
		key = orderid
		if last_key != key and last_key is not None:
			fill_data(lines)
			lines = []
		last_key = key
		lines.append(tpl)
	except:
		print line
		#pass

fill_data(lines)