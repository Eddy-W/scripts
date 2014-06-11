#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
 

from xlrd import open_workbook  
from xlutils.copy import copy
from xlwt import Workbook
from xlwt import Style, XFStyle, Borders, Pattern, Font ,Alignment
from xlwt import easyxf
 
def division_ext(str1,str2):
	res=0
	try:
		res=float(str1)/float(str2)
	except:
		res=0
	return res
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
	style.borders = borders
	#~ style.pattern = pattern
	
	return style
def transCellData(pos,val):
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

	try:
		tmp.append(val)
	except:
		tmp.append(0)
	return tmp,style

def transCellData_ext(pos,val1,val2,val3): #返回(val1-val2)/val3
	tmp=[]
	y=pos[0]
	x=pos[1:]
	tmp.append(int(x)-1)
	tmp.append(ord(y)-65)
	style = getDefualtStyle()
	try:

		tmp.append((float(val1)-float(val2))/float(val3))
	except:
		tmp.append(0)
	return tmp,style
	
def transDataList_imp(res,f,sum1,sum2):
	 
	 
	f=int(f)
	sum_tmp=[]
	list_tmp=[]
	n=len(res)
	r_tmp1=0
	r_tmp2=0
	r_tmp3=0
	r_tmp4=0
	
	
	for r in res:
		 
		r=r.replace("NULL",'0')
		r_val=r.split('\t')
		list_tmp.append([r_val[0],r_val[1],r_val[2],r_val[3]])
		r_tmp1 +=int(r_val[0])
		r_tmp2 +=int(r_val[1])
		r_tmp3 +=int(r_val[2])
		r_tmp4 +=int(r_val[3])
	
	sum_sum_tmp1_=division_ext(r_tmp1,sum1)
	sum_sum_tmp2_=division_ext(r_tmp2,sum2)
 
	
	
	sum_tmp.append(['SUM',r_tmp1,sum_sum_tmp1_,r_tmp2,sum_sum_tmp2_,r_tmp3,r_tmp4])
	list_res=[]
	sum_val=[]
	if(n<=f):
		for i in range(0,n):
			list_sum_tmp1_=division_ext(list_tmp[i][0],sum1)
			list_sum_tmp2_=division_ext(list_tmp[i][1],sum2)

			list_res.append([i+1,int(list_tmp[i][0]),list_sum_tmp1_,int(list_tmp[i][1]),list_sum_tmp2_,int(list_tmp[i][2]),int(list_tmp[i][3])])
		sum_val.append('0%')
		sum_val.append('0%')
	else:
		for i in range(0,f):
			list_sum_tmp1_=division_ext(list_tmp[i][0],sum1)
			list_sum_tmp2_=division_ext(list_tmp[i][1],sum2)
	 
			list_res.append([i+1,int(list_tmp[i][0]),list_sum_tmp1_,int(list_tmp[i][1]),list_sum_tmp2_,int(list_tmp[i][2]),int(list_tmp[i][3])])
		r_tmp1=0
		r_tmp2=0
		r_tmp3=0
		r_tmp4=0
		for i in range(f,n):
			r_tmp1 +=int(list_tmp[i][0])
			r_tmp2 +=int(list_tmp[i][1])
			r_tmp3 +=int(list_tmp[i][2])
			r_tmp4 +=int(list_tmp[i][3])
		list_sum_tmp1_=division_ext(r_tmp1,sum1)
		list_sum_tmp2_=division_ext(r_tmp2,sum2)
 
		list_res.append([-1,r_tmp1,list_sum_tmp1_,r_tmp2,list_sum_tmp2_,r_tmp3,r_tmp4])
		sum_val.append('%0.2f'%(100*list_sum_tmp1_)+'%')
		sum_val.append('%0.2f'%(100*list_sum_tmp2_)+'%')
	 
	return list_res,sum_tmp,sum_val

def transDataList_clk(res,f,sum1,sum2):
	f=int(f)+1
	sum_tmp=[]
	list_tmp=[]
	n=len(res)
	r_tmp1=0
	r_tmp2=0
	r_tmp3=0
	r_tmp4=0
	for r in res:
		 
		r=r.replace('NULL','0')
		r_val=r.split('\t')
		list_tmp.append([r_val[0],r_val[1],r_val[2],r_val[3]])
		r_tmp1 +=int(r_val[0])
		r_tmp2 +=int(r_val[1])
		r_tmp3 +=int(r_val[2])
		r_tmp4 +=int(r_val[3])

	sum_sum_tmp1_=division_ext(r_tmp1,sum1)
	sum_sum_tmp2_=division_ext(r_tmp1,sum1)
	
	sum_tmp.append(['SUM',r_tmp1,sum_sum_tmp1_,r_tmp2,sum_sum_tmp2_,r_tmp3,r_tmp4])

	sum_val=[]
	list_res=[]
	if(n<=f):
		for i in range(0,n):
		 
			list_sum_tmp1_=division_ext(list_tmp[i][0],sum1)
			list_sum_tmp2_=division_ext(list_tmp[i][1],sum2)
			list_res.append([i,int(list_tmp[i][0]),list_sum_tmp1_,int(list_tmp[i][1]),list_sum_tmp2_,int(list_tmp[i][2]),int(list_tmp[i][3])])
		sum_val.append('0%')
		sum_val.append('0%')
	else:
		for i in range(0,f): 
			list_sum_tmp1_=division_ext(list_tmp[i][0],sum1) 
			list_sum_tmp2_=division_ext(list_tmp[i][1],sum2)
			list_res.append([i,int(list_tmp[i][0]),list_sum_tmp1_,int(list_tmp[i][1]),list_sum_tmp2_,int(list_tmp[i][2]),int(list_tmp[i][3])])
		r_tmp1=0
		r_tmp2=0
		r_tmp3=0
		r_tmp4=0
		for i in range(f,n):
			r_tmp1 +=int(list_tmp[i][0])
			r_tmp2 +=int(list_tmp[i][1])
			r_tmp3 +=int(list_tmp[i][2])
			r_tmp4 +=int(list_tmp[i][2])*(i+1) 
		list_sum_tmp1_=division_ext(r_tmp1,sum1) 
		list_sum_tmp2_=division_ext(r_tmp2,sum2)
		list_res.append([-1,r_tmp1,list_sum_tmp1_,r_tmp2,list_sum_tmp2_,r_tmp3,r_tmp4])
		sum_val.append('%0.2f'%(100*list_sum_tmp1_)+'%')
		sum_val.append('%0.2f'%(100*list_sum_tmp2_)+'%')
	 
 
	return list_res,sum_tmp,sum_val
	

def transDataList_referer(res,cnt,sum1,sum2):
	res_tmp=[]
	
	s_val_imp=0
	s_val_clk=0
	for rr in res:
		rr=uniCode(rr)
		(url_tmp,tmp_imp,tmp_clk)=rr.split('\t')
		if tmp_imp=='NULL':
			tmp_imp='0'
		if tmp_clk=='NULL':
			tmp_clk='0'
		s_val_imp+=int(tmp_imp)
		s_val_clk+=int(tmp_clk)
		res1_=division_ext(tmp_imp,sum1) 
		res2_=division_ext(tmp_clk,sum2) 
		res3_=division_ext(tmp_clk,tmp_imp)
		res_tmp.append([url_tmp,res1_,res2_,res3_])
		if(len(res_tmp)>=cnt):
			break
	tmp_f1=division_ext(s_val_imp,sum1)
	tmp_f2=division_ext(s_val_clk,sum2)
	tmp_f3=division_ext(s_val_clk,s_val_imp)
	sum_val=['%0.2f'%(100*tmp_f1)+'%',
	'%0.2f'%(100*tmp_f2)+'%',
	'%0.2f'%(100*tmp_f3)+'%']

	
	return res_tmp,sum_val
def transNULL(res):
	tmp=[]
	for r in res:
		if r=='NULL':
			tmp.append(0)
		else:
			tmp.append(r)
	return tmp
	
def transData_1(res,pars): #显示refer列表
	k_1=['4.refer显示数&点击数']
	tmp=res[k_1[0]]
	if len(tmp)==0:
		return -1
	intindex=2
	data=[]
	 
	for rr in tmp:
		if intindex<=65000:
			rr=uniCode(rr)
			(url_tmp,tmp_imp,tmp_clk)=transNULL(rr.split('\t'))
			data.append(transCellData_str('A%d'%intindex,url_tmp))
			data.append(transCellData('B%d'%intindex,tmp_imp))
			intindex=intindex+1
 
		else:
			break
	return data
def transData_2(res,pars): #排查4一个cookie对应多个ip
	k_1=['11.一个cookie对应多个不同ip数列表']
	tmp=res[k_1[0]]
	if len(tmp)==0:
		return -1
	intindex=2
	data=[]
 
	for rr in tmp:
		if intindex<=65000:
			rr=uniCode(rr)
			(cookie,ip_ns,imp_ns)=transNULL(rr.split('\t'))
			 
			data.append(transCellData_str('A%d'%intindex,cookie))
			data.append(transCellData('B%d'%intindex,imp_ns))
			data.append(transCellData('C%d'%intindex,ip_ns))
			intindex=intindex+1
		else:
			break
	return data	
		
def transData_3(res,pars): #排查5无显示带来的点击cookie
	k_1=['12.无显示cookie']
	tmp=res[k_1[0]]
	if len(tmp)==0:
		return -1
	intindex=2
	data=[]
	for rr in tmp:
		if intindex<=65000:
			rr=uniCode(rr)
			(cookie,ip,clk_ns)=transNULL(rr.split('\t'))
			 
			data.append(transCellData_str('A%d'%intindex,cookie))
			data.append(transCellData('B%d'%intindex,clk_ns))
			data.append(transCellData_str('C%d'%intindex,ip))
			intindex=intindex+1
		else:
			break
	return data	

def transData_4(res,pars): #排查6超过频次设定的显示cookie
	k_1=['13.超过频次设定的显示cookie']
	tmp=res[k_1[0]]
	if len(tmp)==0:
		return -1
	intindex=2
	data=[]
	for rr in tmp:
		if intindex<=65000:
			rr=uniCode(rr)
			(cookie,imp_ns)=transNULL(rr.split('\t'))
			 
			data.append(transCellData_str('A%d'%intindex,cookie))
			data.append(transCellData('B%d'%intindex,imp_ns))
			intindex=intindex+1
		else:
			break
	return data					
		
		
def transData(res,pars):
 
	m_f=int(pars['f'])
	max_refer=4
	k_1=['1.显示cookie唯一数&总数','1.显示ip唯一数&总数','1.点击cookie唯一数&总数','1.点击ip唯一数&总数','1.一个cookie对应各3个不同ip数的个数','1.一个cookie对应各4个不同ip数的个数','1.一个cookie对应各大于4个不同ip数的个数']
	k_2=['2.显示点击重合']
	k_3=['3.点击频次高的cookie所对应的显示数','3.显示频次高的cookie所对应的点击数']#
	k_4=['4.refer显示数&点击数']
	 
	data=[]
	(imp_cookie_dis,imp_cookie_all)=transNULL(res[k_1[0]][0].split('\t'))
	(imp_ip_dis,imp_ip_all)=transNULL(res[k_1[1]][0].split('\t'))
	(clk_cookie_dis,clk_cookie_all)=transNULL(res[k_1[2]][0].split('\t'))
	 
	 
 
	(clk_ip_dis,clk_ip_all)=transNULL(res[k_1[3]][0].split('\t'))
	(one_cookie_3_ip_dis,one_cookie_3_ip_all)=transNULL(res[k_1[4]][0].split('\t'))
	(one_cookie_4_ip_dis,one_cookie_4_ip_all)=transNULL(res[k_1[5]][0].split('\t'))
	(one_cookie_b_4_ip_dis,one_cookie_b_4_ip_all)=transNULL(res[k_1[6]][0].split('\t'))
	(imp_join_clk_cookie_dis,imp_join_clk_cookie_clk)=transNULL(res[k_2[0]][0].split('\t'))
	(tb_imp_f_list,tb_imp_f_sum,sum_imp_str)=transDataList_imp(res[k_3[0]],m_f,imp_cookie_dis,imp_cookie_all)
	(tb_clk_f_list,tb_clk_f_sum,sum_clk_str)=transDataList_clk(res[k_3[1]],m_f,imp_cookie_dis,imp_cookie_all)
	(tb_referer_list,sum_referer_str)=transDataList_referer(res[k_4[0]],max_refer,imp_cookie_all,clk_cookie_all)

	#排查3
	data.append(transCellData('A5',imp_ip_all))#IP显示总数
	data.append(transCellData('B5',imp_cookie_all))#cookie显示总数
	data.append(transCellData_ext('C5',imp_ip_all,imp_cookie_all,1))#显示差值
	data.append(transCellData('D5',clk_ip_all))#IP点击总数
	data.append(transCellData('E5',clk_cookie_all))#cookie点击总数
	data.append(transCellData_ext('F5',clk_ip_all,clk_cookie_all,1))#点击差值
	#排查3 判断
	conclusion_str=''
	if  imp_ip_all==imp_cookie_all and clk_ip_all==clk_cookie_all:
		conclusion_str='正常'
	else:
		conclusion_str='异常'
	conclusion_str=uniCode(conclusion_str)
	ts=transCellData_summary('D2',conclusion_str)
	##ts[1].borders.right = Borders.THICK ?
	ts[1].alignment.vert = Alignment.VERT_CENTER

	data.append(ts)
	
	data.append(transCellData('A10',imp_ip_dis))#IP唯一显示数
	data.append(transCellData('B10',imp_cookie_dis))#cookie唯一显示数
	data.append(transCellData_ext('C10',imp_cookie_dis,imp_ip_dis,1))#显示差异比例
	data.append(transCellData('D10',clk_ip_dis))#IP唯一点击数
	data.append(transCellData('E10',clk_cookie_dis))#cookie唯一点击数
	data.append(transCellData_ext('F10',clk_cookie_dis,clk_ip_dis,1))#点击差异比例
	ts=transCellData_ext('A12',one_cookie_3_ip_dis,0,imp_cookie_dis)
	ts[1].num_format_str='0.00%'
	data.append(ts)#一个cookie对应3个ip的cookie占比
	ts=transCellData_ext('B12',one_cookie_3_ip_all,0,imp_cookie_all)
	ts[1].num_format_str='0.00%'
	data.append(ts)#其显示占比
	ts=transCellData_ext('C12',one_cookie_4_ip_dis,0,imp_cookie_dis)
	ts[1].num_format_str='0.00%'
	data.append(ts)#一个cookie对应4个ip的cookie占比
	ts=transCellData_ext('D12',one_cookie_4_ip_all,0,imp_cookie_all)
	ts[1].num_format_str='0.00%'
	data.append(ts)#其显示占比
	ts=transCellData_ext('E12',one_cookie_b_4_ip_dis,0,imp_cookie_dis)
	ts[1].num_format_str='0.00%'
	data.append(ts)#一个cookie对应4个ip以上的cookie占比
	ts=transCellData_ext('F12',one_cookie_b_4_ip_all,0,imp_cookie_all)
	ts[1].num_format_str='0.00%'
	data.append(ts)#其显示占比
	

	 
	data.append(transCellData('A19',clk_cookie_all))#点击总数
	data.append(transCellData('B19',imp_cookie_all))#显示cookie总数(唯一)
	data.append(transCellData('C19',clk_cookie_all))#点击cookie总数(唯一)
	data.append(transCellData_ext('D19',clk_cookie_all,imp_cookie_all,1 ))#cookie重合数1
	data.append(transCellData('E19',imp_join_clk_cookie_clk))#cookie重合数2
	data.append(transCellData_ext('F19',clk_cookie_all,imp_join_clk_cookie_clk,1))	#无显示cookie
	ts=transCellData_ext('G19',clk_cookie_dis,imp_join_clk_cookie_dis,clk_cookie_dis)
	ts[1].num_format_str='0.00%'
	data.append(ts)	#无显示cookie占比
	ts=transCellData_ext('H19',clk_cookie_all,imp_join_clk_cookie_clk,clk_cookie_all)
	ts[1].num_format_str='0.00%'
	data.append(ts)	#点击数占总体
	
	intindex=27
 
	for tt in tb_imp_f_list:	 
		ts=transCellData('A%d'%intindex,tt[0])
		if tt[0]<0:
			ts[0][2]= len(tb_imp_f_list)-1		
			ts[1].num_format_str='0+'
		data.append(ts)
		data.append(transCellData('B%d'%intindex,tt[1]))
		ts=transCellData_ext('C%d'%intindex,tt[2],0,1)
		ts[1].num_format_str='0.00%'		 
		data.append(ts)
		data.append(transCellData('D%d'%intindex,tt[3]))
		ts=transCellData_ext('E%d'%intindex,tt[4],0,1)
		ts[1].num_format_str='0.00%'
		data.append(ts)
		data.append(transCellData('F%d'%intindex,tt[5]))
		data.append(transCellData('G%d'%intindex,tt[6]))
		intindex+=1
	intindex=37 
  
	pattern = Pattern()
	pattern.pattern = Style.pattern_map['solid']
	###pattern.pattern_back_colour = 0xBFBFBF
	pattern.pattern_fore_colour = 0x37
	
	ts=transCellData('B%d'%intindex,tb_imp_f_sum[0][1])
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData_ext('C%d'%intindex,tb_imp_f_sum[0][2],0,1)
	ts[1].num_format_str='0.00%'
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData('D%d'%intindex,tb_imp_f_sum[0][3])
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData_ext('E%d'%intindex,tb_imp_f_sum[0][4],0,1)
	ts[1].num_format_str='0.00%'
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData('F%d'%intindex,tb_imp_f_sum[0][5])
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData('G%d'%intindex,tb_imp_f_sum[0][6])
	ts[1].pattern=pattern
	data.append(ts)
	
	intindex=39
 
	for tt in tb_clk_f_list:
		ts=transCellData('A%d'%intindex,tt[0])
		if tt[0]<0:
			ts[0][2]= len(tb_imp_f_list)-1		
			ts[1].num_format_str='0+'
		data.append(ts)
		data.append(transCellData('B%d'%intindex,tt[1]))
		ts=transCellData_ext('C%d'%intindex,tt[2],0,1)
		ts[1].num_format_str='0.00%'
		data.append(ts)
		data.append(transCellData('D%d'%intindex,tt[3]))
		ts=transCellData_ext('E%d'%intindex,tt[4],0,1)
		ts[1].num_format_str='0.00%'
		data.append(ts)
		data.append(transCellData('F%d'%intindex,tt[5]))
		data.append(transCellData('G%d'%intindex,tt[6]))
		intindex+=1
	intindex=49
	 
	ts=transCellData('B%d'%intindex,tb_clk_f_sum[0][1])
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData_ext('C%d'%intindex,tb_clk_f_sum[0][2],0,1)
	ts[1].num_format_str='0.00%'
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData('D%d'%intindex,tb_clk_f_sum[0][3])
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData_ext('E%d'%intindex,tb_clk_f_sum[0][4],0,1)
	ts[1].num_format_str='0.00%'
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData('F%d'%intindex,tb_clk_f_sum[0][5])
	ts[1].pattern=pattern
	data.append(ts)
	ts=transCellData('G%d'%intindex,tb_clk_f_sum[0][6])
	ts[1].pattern=pattern
	data.append(ts)
	
 
	
	intindex=53
	for tt in tb_referer_list:
		 
		data.append(transCellData_str('B%d'%intindex,tt[0]))
		ts=transCellData_ext('E%d'%intindex,tt[1],0,1)
		ts[1].num_format_str='0.00%'
		data.append(ts)	
		ts=transCellData_ext('G%d'%intindex,tt[2],0,1)
		ts[1].num_format_str='0.00%'
		data.append(ts)
		ts=transCellData_ext('I%d'%intindex,tt[3],0,1)
		ts[1].num_format_str='0.00%'
		data.append(ts)
		intindex+=1
	
	##################summary####################
 
	test_str='有%d个url产生了%s的显示及%s的点击，点击率约%s'%(max_refer,sum_referer_str[0],sum_referer_str[1],sum_referer_str[2])
	test_str=uniCode(test_str)
	ts=transCellData_summary('D52',test_str)
	data.append(ts)
	
	 
	test_str='%d频次以上Cookie占比%s；\n%d频次以上显示数占比%s'%(m_f,sum_imp_str[0],m_f,sum_imp_str[1])
	test_str=uniCode(test_str)
	ts=transCellData_summary('D24',test_str)
	ts[1].borders.right = Borders.NO_LINE
	ts[1].borders.bottom  = Borders.NO_LINE
	ts[1].alignment.wrap = 1
	data.append(ts)
	
	test_str='点击%d次及以上的Cookie占比%s；\n点击%d次及以上的点击数占比%s'%(m_f,sum_clk_str[0],m_f,sum_clk_str[1])
	test_str=uniCode(test_str)
	ts=transCellData_summary('D25',test_str)
	ts[1].borders.right = Borders.NO_LINE
	ts[1].borders.top  = Borders.NO_LINE
	ts[1].alignment.wrap = 1
	data.append(ts)
	
	
	if (float(clk_cookie_dis)==0):
		test_str='没有点击Cookie'
	else:
		if clk_cookie_dis!=0:
			val1_=100*(float(clk_cookie_dis)-float(imp_join_clk_cookie_dis))/float(clk_cookie_dis)
		else:
			val1_=0
		if clk_cookie_all!=0:
			val2_=100*(float(clk_cookie_all)-float(imp_join_clk_cookie_clk))/float(clk_cookie_all)
		else:
			val2_=0
 
		test_str='%s的点击Cookie没有显示；点击数占总比%s'%('%0.2f'%val1_+'%','%0.2f'%val2_+'%')
	test_str=uniCode(test_str)
	ts=transCellData_summary('D17',test_str)
	##ts[1].borders.right = Borders.THICK ?
	ts[1].alignment.vert = Alignment.VERT_CENTER

	data.append(ts)
	
	 
	 
	val1_=100*sumPercent([one_cookie_3_ip_dis ,one_cookie_4_ip_dis ,one_cookie_b_4_ip_dis ],imp_cookie_dis)
	val2_=100*sumPercent([one_cookie_3_ip_all,  one_cookie_4_ip_all,  one_cookie_b_4_ip_all],  imp_cookie_all)
	
	test_str='1个Cookie对应2个以上IP的Cookie占比%s;\n显示占总体%s'%('%0.2f'%val1_+'%','%0.2f'%val2_+'%')
	test_str=uniCode(test_str)
	ts=transCellData_summary('D8',test_str)
	ts[1].alignment.wrap = 1
	data.append(ts)
	
	
	
	return data
def uniCode(str_):
	return unicode(str_, "utf-8")
def sumPercent(vals,sum):
	vsum=0
	for v in vals:
		if v!='NULL':
			vsum+=float(v)
	if float(sum)==0:
		return 0
	else:
		return division_ext(vsum,sum)
	
def saveRes(res,pars):

	try:
		#output_filename = r'xxxx.xls' 
		output_filename = r'%s'%pars['output']
		data_main=transData(res,pars)

		#data=transData(res)
		
		#row = easyxf('pattern: pattern solid, fore_colour blue')
		col = easyxf('pattern: pattern solid, fore_colour green')
		#sheet.row(i).set_style(row)
		
		
		#~ sheet.write(1,1,date(2009,3,18),easyxf(
			#~ 'font: name Arial;'
			#~ 'borders: left thick, right thick, top thick, bottom
			#~ thick;'
			#~ 'pattern: pattern solid, fore_colour red;',
			#~ num_format_str='YYYY-MM-DD'
			#~ ))
		
		rb = open_workbook(r'templet.xls', formatting_info=True )
		wb = copy(rb)  
		ws_main = wb.get_sheet(0)
		
		from xlutils.styles import Styles
		for item in data_main:
			posx=item[0][0]
			posy=item[0][1]
			val=item[0][2]
			syl=item[1]
			ws_main.write(posx,posy, val, syl )
			#ws.write(item[0], item[1], 12.2, style )
		
		#显示refer列表
		data_s1=transData_1(res,pars)
		if data_s1!=-1:
			ws_s1 = wb.get_sheet(1)
			for item in data_s1:
				posx=item[0][0]
				posy=item[0][1]
				val=item[0][2]
				syl=item[1]
				ws_s1.write(posx,posy, val, syl )
		#排查4一个cookie对应多个ip
		data_s2=transData_2(res,pars)
		if data_s2!=-1:
			ws_s2 = wb.get_sheet(2)
			for item in data_s2:
				posx=item[0][0]
				posy=item[0][1]
				val=item[0][2]
				syl=item[1]
				ws_s2.write(posx,posy, val, syl )
		#排查5无显示带来的点击cookie
		data_s3=transData_3(res,pars)
		if data_s3!=-1:
			ws_s3 = wb.get_sheet(3)
			for item in data_s3:
				posx=item[0][0]
				posy=item[0][1]
				val=item[0][2]
				syl=item[1]
				ws_s3.write(posx,posy, val, syl )
		#排查6超过频次设定的显示cookie
		data_s4=transData_4(res,pars)
		if data_s4!=-1:
			ws_s4 = wb.get_sheet(4)
			for item in data_s4:
				posx=item[0][0]
				posy=item[0][1]
				val=item[0][2]
				syl=item[1]
				ws_s4.write(posx,posy, val, syl )
		 
		#ws.col(0).width_mismatch=0
		wb.save(output_filename)  
		return 'OK'
	except Exception,e:
		return e
		
 
