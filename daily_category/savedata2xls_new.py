#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-


import os
import sys
 
fp='res'
dt= sys.argv[1]
sys.path.append(r'/home/group_dataanalysis/wangwentao/daily_category/lib/')
 

from xlrd import open_workbook  
from xlutils.copy import copy
from xlwt import Workbook
from xlwt import Style, XFStyle, Borders, Pattern, Font ,Alignment
from xlwt import easyxf
from xlutils.styles import Styles



def getDefualtStyle():
	fnt = Font()
	fnt.name = 'Arial'
	
	borders = Borders()
	borders.left = Borders.THIN
	borders.right = Borders.THIN
	borders.top = Borders.THIN
	borders.bottom = Borders.THIN
	
	#pattern = Pattern()
	#pattern.pattern = Style.pattern_map['solid']
	###pattern.pattern_back_colour = 0xBFBFBF
	#pattern.pattern_fore_colour = 0x37
	
	
	alignment = Alignment()
	#alignment.horizontal = Alignment.HORZ_LEFT 
	alignment.horizontal = Alignment.HORZ_RIGHT
	
	
	style = XFStyle()
	#~ style.num_format_str='0.000%'
	#~ style.num_format_str='0+'
	#~ style.font = fnt
	style.align = alignment
	#style.borders = borders
	#~ style.pattern = pattern
	
	return style

def uniCode(str_):
	return unicode(str_, "utf-8")

def transCellData_cstr(pos,val):
	if val=='NULL':
		val='-'
	val=uniCode(val)
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	style = getDefualtStyle()
	try:
		tmp.append(val)
	except:
		tmp.append(0)
	return tmp,style
	
def transCellData_str(pos,val):
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	style = getDefualtStyle()
	try:
		tmp.append(val)
	except:
		tmp.append(0)
	return tmp,style

def transCellData_int(pos,val):
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	
	try:
		tmp.append(int(val))
	except:
		tmp.append(0)
	return tmp,style 
	
	
def transCellData_bigint(pos,val):
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	style.num_format_str= r'#,##0'
	try:
		tmp.append(int(val))
	except:
		tmp.append(0)
	return tmp,style 


def transCellData_float(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
	
def transCellData_float_percent(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	style.num_format_str='0.00%'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
def transCellData_float_percent_3bit(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	style.num_format_str='0.000%'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
	
def transCellData_float_percent_0bit(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	style.num_format_str='0%'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style
def transCellData_summary(pos,val):
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	style = getDefualtStyle()
	style.font.colour_index =0x17
	style.alignment.horz  = Alignment.HORZ_CENTER
	#style.borders.right = Borders.NO_LINE
	style.font.bold =True
	borders = Borders()
	borders.left = Borders.THIN
	borders.right = Borders.THIN
	borders.top = Borders.THIN
	borders.bottom = Borders.THIN
	style.borders = borders
	try:
		tmp.append(val)
	except:
		tmp.append(0)
	return tmp,style
def transCellData_float_2bit(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	style.num_format_str='0.00'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
def transCellData_float_3bit(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
 
	tmp.append(transpos(pos)[0])
	tmp.append(transpos(pos)[1])
	
	style = getDefualtStyle()
	style.num_format_str='0.000'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
def transpos(pos):
	tmp=pos.split(':')
	y=tmp[0]
	x=tmp[1]
	res=[]
	res.append(int(x)-1)
	
	cnt=0;
	n=len(y)
	for i in range(0,n):
		cnt=cnt+ord(y[i])-65
		
	res.append(cnt+(n-1)*26)
	return res
	
def transtop(topstr):
	res=[]
	tmp1=topstr.split('|')
	
	for tt in tmp1:
		
		if len(tt)>0:
			t=tt.split(':')
			
			if t[1]!='' and int(t[1])>0:
				res.append(t)
	return res
def divide(a,b):
	if a=='0' or b=='0':
		return 0
	else:
		return float(a)/float(b)
		
def transData_0(fpx):
	data=[]
	fin=open(r'./%s/size.txt'%(fpx))
	
 
 
	intindex=4
	for line in fin.readlines():
		line=line[:-1].replace('\x01','\t')
		line=line.replace('\\N','0')
		
		(size,pos,reqns,topreq,topclk,bidns,impns,clkns,wp,bidrate,winrate,ctr,cpm,cpc,avgbidfloor,avgbidprice,b0,b1,b2,b3,b4,b5)=line.split('\t')
	 
 
		data.append(transCellData_str('A:%d'%intindex,size))
		data.append(transCellData_str('B:%d'%intindex,pos))

		data.append(transCellData_bigint('C:%d'%intindex,reqns))
		data.append(transCellData_bigint('D:%d'%intindex,bidns))
		data.append(transCellData_float_percent('E:%d'%intindex,bidrate)) 
		data.append(transCellData_bigint('F:%d'%intindex,impns))
		data.append(transCellData_float_percent('G:%d'%intindex,winrate))
		data.append(transCellData_bigint('H:%d'%intindex,clkns))
		data.append(transCellData_float_percent_3bit('I:%d'%intindex,ctr))
		
		data.append(transCellData_float_2bit('J:%d'%intindex,wp))
		data.append(transCellData_float_2bit('K:%d'%intindex,cpm))
		data.append(transCellData_float_2bit('L:%d'%intindex,cpc))
		data.append(transCellData_float_2bit('M:%d'%intindex,avgbidfloor))
	
		data.append(transCellData_float_2bit('N:%d'%intindex,avgbidprice))
		
		data.append(transCellData_bigint('O:%d'%intindex,b0))
		data.append(transCellData_bigint('P:%d'%intindex,b1))
		data.append(transCellData_bigint('Q:%d'%intindex,b2))
		data.append(transCellData_bigint('R:%d'%intindex,b3))
		data.append(transCellData_bigint('S:%d'%intindex,b4))
		data.append(transCellData_bigint('T:%d'%intindex,b5))
		
		pos=['U','V','W','X','Y','Z','AA','AB','AC','AD']
		datatop=transtop(topreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],reqns)))
			cnt=cnt+1;
			
		pos=['AE','AF','AG','AH','AI','AJ','AK','AL','AM','AN']
		datatop=transtop(topclk)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_bigint('%s:%d'%(pos[cnt],intindex),d[1]))
			cnt=cnt+1;
		 
		intindex+=1
	return data

def transData_1(fpx):
	data=[]
	fin=open(r'./%s/region.txt'%(fpx))
	
 
 
	intindex=4
	for line in fin.readlines():
		line=line[:-1].replace('\x01','\t')
		line=line.replace('\\N','0')
		
		(region,reqns,topreq,topclk,bidns,impns,clkns,wp,bidrate,winrate,ctr,cpm,cpc,avgbidfloor,avgbidprice,b0,b1,b2,b3,b4,b5)=line.split('\t')
	
		
		
		
 
		data.append(transCellData_str('A:%d'%intindex,region))

		data.append(transCellData_bigint('B:%d'%intindex,reqns))
		data.append(transCellData_bigint('C:%d'%intindex,bidns))
		data.append(transCellData_float_percent('D:%d'%intindex,bidrate)) 
		data.append(transCellData_bigint('E:%d'%intindex,impns))
		data.append(transCellData_float_percent('F:%d'%intindex,winrate))
		data.append(transCellData_bigint('G:%d'%intindex,clkns))
		data.append(transCellData_float_percent_3bit('H:%d'%intindex,ctr))
		
		data.append(transCellData_float_2bit('I:%d'%intindex,wp))
		data.append(transCellData_float_2bit('J:%d'%intindex,cpm))
		data.append(transCellData_float_2bit('K:%d'%intindex,cpc))
		data.append(transCellData_float_2bit('L:%d'%intindex,avgbidfloor))
	
		data.append(transCellData_float_2bit('M:%d'%intindex,avgbidprice))
		
		data.append(transCellData_bigint('N:%d'%intindex,b0))
		data.append(transCellData_bigint('O:%d'%intindex,b1))
		data.append(transCellData_bigint('P:%d'%intindex,b2))
		data.append(transCellData_bigint('Q:%d'%intindex,b3))
		data.append(transCellData_bigint('R:%d'%intindex,b4))
		data.append(transCellData_bigint('S:%d'%intindex,b5))
		
		pos=['T','U','V','W','X','Y','Z','AA','AB','AC']
		datatop=transtop(topreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],reqns)))
			cnt=cnt+1;
			
		pos=['AD','AE','AF','AG','AH','AI','AJ','AK','AL','AM']
		datatop=transtop(topclk)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_bigint('%s:%d'%(pos[cnt],intindex),d[1]))
			cnt=cnt+1;
		 
		intindex+=1
	return data						
def transData_2(fpx):
	data=[]
	fin=open(r'./%s/domain.txt'%(fpx))
	
 
 
	intindex=4
	for line in fin.readlines():
		line=line[:-1].replace('\x01','\t')
		line=line.replace('\\N','0')
		
	

		(domain,reqns,bidns,impns,clkns,wp,bidrate,winrate,ctr,cpm,cpc,avgbidfloor,avgbidprice,b0,b1,b2,b3,b4,b5,dreqns,topreq,topclk)=line.split('\t')
		datatop=transtop(topreq)
		flag=False
		for d in datatop:
			if d[0]=="Books & Literature":
				if divide(d[1],dreqns)>0.5:
					flag=True
					break
				else:
					flag=False
					break
		if flag==False:
			continue
				
 
		data.append(transCellData_str('A:%d'%intindex,domain))

		data.append(transCellData_bigint('B:%d'%intindex,reqns))
		data.append(transCellData_bigint('C:%d'%intindex,bidns))
		data.append(transCellData_float_percent('D:%d'%intindex,bidrate)) 
		data.append(transCellData_bigint('E:%d'%intindex,impns))
		data.append(transCellData_float_percent('F:%d'%intindex,winrate))
		data.append(transCellData_bigint('G:%d'%intindex,clkns))
		data.append(transCellData_float_percent_3bit('H:%d'%intindex,ctr))
		
		data.append(transCellData_float_2bit('I:%d'%intindex,wp))
		data.append(transCellData_float_2bit('J:%d'%intindex,cpm))
		data.append(transCellData_float_2bit('K:%d'%intindex,cpc))
		data.append(transCellData_float_2bit('L:%d'%intindex,avgbidfloor))
	
		data.append(transCellData_float_2bit('M:%d'%intindex,avgbidprice))
		
		data.append(transCellData_bigint('N:%d'%intindex,b0))
		data.append(transCellData_bigint('O:%d'%intindex,b1))
		data.append(transCellData_bigint('P:%d'%intindex,b2))
		data.append(transCellData_bigint('Q:%d'%intindex,b3))
		data.append(transCellData_bigint('R:%d'%intindex,b4))
		data.append(transCellData_bigint('S:%d'%intindex,b5))
		
		
		data.append(transCellData_bigint('T:%d'%intindex,dreqns))
		pos=['U','V','W','X','Y','Z','AA','AB','AC','AD']
		datatop=transtop(topreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],dreqns)))
			cnt=cnt+1;
			
		pos=['AE','AF','AG','AH','AI','AJ','AK','AL','AM','AN']
		datatop=transtop(topclk)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_bigint('%s:%d'%(pos[cnt],intindex),d[1]))
			cnt=cnt+1;
		 
		intindex+=1
	return data



def transData_3(fpx):
	data=[]
	fin=open(r'./%s/bigcategory.txt'%(fpx))
	
 
 
	intindex=4
	for line in fin.readlines():
		line=line[:-1].replace('\x01','\t')
		line=line.replace('\\N','0')
		
 
 

		(parent,reqns,bidns,impns,clkns,wp,bidrate,winrate,ctr,cpm,cpc,avgbidfloor,avgbidprice,c0,c1,c2,c3,c4,dtopreq,dtopclk,ptopreq)=line.split('\t')
	 
 
		data.append(transCellData_str('A:%d'%intindex,parent))

		data.append(transCellData_bigint('B:%d'%intindex,reqns))
		data.append(transCellData_bigint('C:%d'%intindex,bidns))
		data.append(transCellData_float_percent('D:%d'%intindex,bidrate)) 
		data.append(transCellData_bigint('E:%d'%intindex,impns))
		data.append(transCellData_float_percent('F:%d'%intindex,winrate))
		data.append(transCellData_bigint('G:%d'%intindex,clkns))
		data.append(transCellData_float_percent_3bit('H:%d'%intindex,ctr))
		
		data.append(transCellData_float_2bit('I:%d'%intindex,wp))
		data.append(transCellData_float_2bit('J:%d'%intindex,cpm))
		data.append(transCellData_float_2bit('K:%d'%intindex,cpc))
		data.append(transCellData_float_2bit('L:%d'%intindex,avgbidfloor))
	
		data.append(transCellData_float_2bit('M:%d'%intindex,avgbidprice))
		
		data.append(transCellData_bigint('N:%d'%intindex,c0))
		data.append(transCellData_bigint('O:%d'%intindex,c1))
		data.append(transCellData_bigint('P:%d'%intindex,c2))
		data.append(transCellData_bigint('Q:%d'%intindex,c3))
		data.append(transCellData_bigint('R:%d'%intindex,c4))
 
		pos=['S','T','U','V','W','X','Y','Z','AA','AB']
		datatop=transtop(dtopreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],reqns)))
			cnt=cnt+1;
			
		pos=['AC','AD','AE','AF','AG','AH','AI','AJ','AK','AL']
		datatop=transtop(dtopclk)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_bigint('%s:%d'%(pos[cnt],intindex),d[1]))
			cnt=cnt+1;
		 
		pos=['AM','AN','AO','AP','AQ','AR','AS','AT','AU','AV']
		datatop=transtop(ptopreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],reqns)))
			cnt=cnt+1;
		intindex+=1
	return data


def transData_4(fpx):
	data=[]
	fin=open(r'./%s/smallcategory.txt'%(fpx))
	
 
 
	intindex=4
	for line in fin.readlines():
		line=line[:-1].replace('\x01','\t')
		line=line.replace('\\N','0')
		
 
 

		(self,parent,nativecode,reqns,bidns,impns,clkns,wp,bidrate,winrate,ctr,cpm,cpc,avgbidfloor,avgbidprice,c0,c1,c2,c3,c4,dtopreq,dtopclk,ptopreq)=line.split('\t')
	 
 
		data.append(transCellData_cstr('A:%d'%intindex,self))
		data.append(transCellData_str('B:%d'%intindex,parent))
		data.append(transCellData_str('C:%d'%intindex,nativecode))

		data.append(transCellData_bigint('D:%d'%intindex,reqns))
		data.append(transCellData_bigint('E:%d'%intindex,bidns))
		data.append(transCellData_float_percent('F:%d'%intindex,bidrate)) 
		data.append(transCellData_bigint('G:%d'%intindex,impns))
		data.append(transCellData_float_percent('H:%d'%intindex,winrate))
		data.append(transCellData_bigint('I:%d'%intindex,clkns))
		data.append(transCellData_float_percent_3bit('J:%d'%intindex,ctr))
		
		data.append(transCellData_float_2bit('K:%d'%intindex,wp))
		data.append(transCellData_float_2bit('L:%d'%intindex,cpm))
		data.append(transCellData_float_2bit('M:%d'%intindex,cpc))
		data.append(transCellData_float_2bit('N:%d'%intindex,avgbidfloor))
	
		data.append(transCellData_float_2bit('O:%d'%intindex,avgbidprice))
		
		data.append(transCellData_bigint('P:%d'%intindex,c0))
		data.append(transCellData_bigint('Q:%d'%intindex,c1))
		data.append(transCellData_bigint('R:%d'%intindex,c2))
		data.append(transCellData_bigint('S:%d'%intindex,c3))
		data.append(transCellData_bigint('T:%d'%intindex,c4))
 
		pos=['U','V','W','X','Y','Z','AA','AB','AC','AD']
		datatop=transtop(dtopreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],reqns)))
			cnt=cnt+1;
			
		pos=['AE','AF','AG','AH','AI','AJ','AK','AL','AM','AN']
		datatop=transtop(dtopclk)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_bigint('%s:%d'%(pos[cnt],intindex),d[1]))
			cnt=cnt+1;
		 
		pos=['AO','AP','AQ','AR','AS','AT','AU','AV','AW','AX']
		datatop=transtop(ptopreq)
		cnt=0;
		for d in datatop:
			data.append(transCellData_str('%s:%d'%(pos[cnt],intindex),d[0]))
			cnt=cnt+1;
			data.append(transCellData_float_percent_0bit('%s:%d'%(pos[cnt],intindex),divide(d[1],reqns)))
			cnt=cnt+1;
		intindex+=1
	return data







output_filename = r'./%s/adx_report_%s.xls'%(fp,dt)

rb = open_workbook(r'adx_template.xls', formatting_info=True )
wb = copy(rb)  

ws_0 = wb.get_sheet(0)
data_0=transData_0(fp)		
for item in data_0:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_0.write(posx,posy, val, syl )

ws_1 = wb.get_sheet(1)
data_1=transData_1(fp)		
for item in data_1:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_1.write(posx,posy, val, syl )

ws_2 = wb.get_sheet(2)
data_2=transData_2(fp)		
for item in data_2:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_2.write(posx,posy, val, syl )


ws_3 = wb.get_sheet(3)
data_3=transData_3(fp)		
for item in data_3:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_3.write(posx,posy, val, syl )


ws_4 = wb.get_sheet(4)
data_4=transData_4(fp)		
for item in data_4:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_4.write(posx,posy, val, syl )		
		
wb.save(output_filename)  





