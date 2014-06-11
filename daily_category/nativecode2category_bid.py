#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys, os
 
def calculatewegbin(weg):
	weg=float(weg)
	ind=4
	if weg!=1.0:
		ind=int(weg/0.2)
	res=''
	for i in range(0,5):
		if i==ind:
			res+=str(weg)+'\t'
		else:
			res+='0\t'
	return res[:-1]


for line in sys.stdin:
	try:
		(geoid,domain,sz,bidfloor,cateid,cateweg,bidprice,imptag,winprice,clktag) = line.strip('\n').split('\t')

		cateids=cateid[1:-1].split(',')
		catewegs=cateweg[1:-1].split(',')

		for code in cateids:
			 
			sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"%(geoid,domain,bidfloor,code,bidprice,imptag,winprice,clktag))
		
		
	except:
		pass
		
		