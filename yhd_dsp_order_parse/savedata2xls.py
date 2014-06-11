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

fp=sys.argv[1]
dt=sys.argv[2]
dd=sys.argv[3]

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
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	style = getDefualtStyle()
	try:
		tmp.append(val)
	except:
		tmp.append(0)
	return tmp,style
	
def transCellData_str(pos,val):
	tmp=[]
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	style = getDefualtStyle()
	try:
		tmp.append(val)
	except:
		tmp.append(0)
	return tmp,style

def transCellData_int(pos,val):
	tmp=[]
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	
	style = getDefualtStyle()
	
	try:
		tmp.append(int(val))
	except:
		tmp.append(0)
	return tmp,style 

def transCellData_float(pos,val):
	if val=='NaN':
		val=0
	tmp=[]
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	
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
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	
	style = getDefualtStyle()
	style.num_format_str='0.00%'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
def transCellData_summary(pos,val):
	tmp=[]
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
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
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	
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
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	
	style = getDefualtStyle()
	style.num_format_str='0.000'
	try:
		tmp.append(float(val))
	except:
		tmp.append(0)
	return tmp,style 
def transData_2(fpx,dd,ws):
	data=[]
	fin=open('%s/output-3'%(fpx))
	intindex=3
	sum=0
	roisum=0
	roicnt=0
	for line in fin.readlines():
		#line=line[:-1].replace('\x01','\t')
		#line=line.replace('\\N','NULL')
		
		line=line.strip('\n')
		(sid,sname,bid,bname,order_ts,orderid,ordervalue,statuscod,status,isroi)=line.split('\t')
		
		sum+=float(ordervalue)
		if isroi=='Yes':
			roisum+=float(ordervalue)
			roicnt+=1
		data.append(transCellData_str('A%d'%intindex,dd))
		data.append(transCellData_int('B%d'%intindex,sid))
		data.append(transCellData_cstr('C%d'%intindex,sname))
		data.append(transCellData_int('D%d'%intindex,bid))
		data.append(transCellData_cstr('E%d'%intindex,bname))
 	
		
		data.append(transCellData_str('G%d'%intindex,orderid))
		data.append(transCellData_str('F%d'%intindex,order_ts))
		data.append(transCellData_str('H%d'%intindex,int(ordervalue)))
		data.append(transCellData_cstr('I%d'%intindex,status))
		data.append(transCellData_str('J%d'%intindex,statuscod))
		intindex=intindex+1
 
	test_str='%s产生%d个订单,订单金额共计%d;产生%d个有效订单,有效订单金额共计%d,ROI约为%0.2f'%(dd,(intindex-3),sum,roicnt,roisum,roisum/ws)
	test_str=uniCode(test_str)
	ts=transCellData_summary('A1',test_str)
	ts[1].alignment.vert = Alignment.VERT_CENTER
	ts[1].alignment.wrap = 1
	data.append(ts)
	
 
	 
	return data
def transData_1(fpx,dd):
	data=[]
	fin=open('%s/output-1'%(fpx))

 
	intindex=3
	costsum=0
	for line in fin.readlines():
		
		line=line[:-1].replace('\x01','\t')
		line=line.replace('\\N','NULL') 
		(sid,sname,bidn,winn,winrate,clk,ctr,ws,cpm,cpc,avgbidfloor,avgbidprice,ordern,ordersum)=line.split('\t')
		if int(bidn)>100:
			data.append(transCellData_str('A%d'%intindex,dd))
			data.append(transCellData_int('B%d'%intindex,sid))
			data.append(transCellData_cstr('C%d'%intindex,sname))
			data.append(transCellData_int('D%d'%intindex,bidn))
			data.append(transCellData_int('E%d'%intindex,winn))
			data.append(transCellData_float_percent('F%d'%intindex,winrate))
			data.append(transCellData_float_2bit('G%d'%intindex,ws))
			data.append(transCellData_int('H%d'%intindex,winn))
			data.append(transCellData_int('I%d'%intindex,clk))
			data.append(transCellData_float_percent('J%d'%intindex,ctr))
			data.append(transCellData_float_2bit('K%d'%intindex,cpm))
			data.append(transCellData_float_2bit('L%d'%intindex,cpc))
			data.append(transCellData_float_2bit('M%d'%intindex,avgbidfloor))
			data.append(transCellData_float_2bit('N%d'%intindex,avgbidprice))
			
			data.append(transCellData_int('O%d'%intindex,ordern))
			data.append(transCellData_float_2bit('P%d'%intindex,ordersum))
		  
	 
		  
		
			try:
			       costsum+=float(ws)
			except:
			       pass
			intindex+=1
	return data,costsum
 
	
output_filename = r'%s/yhd_dsp_report_%s.xls'%(fp,dt)

rb = open_workbook(r'yhd_dsp_template.xls', formatting_info=True )
wb = copy(rb)  

 
ws_1 = wb.get_sheet(0)
data_2,ws=transData_1(fp,dd)		
for item in data_2:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_1.write(posx,posy, val, syl )
ws_2 = wb.get_sheet(1)
data_3=transData_2(fp,dd,ws)		
for item in data_3:
		posx=item[0][0]
		posy=item[0][1]
		val=item[0][2]
		syl=item[1]
		ws_2.write(posx,posy, val, syl )

wb.save(output_filename)  





