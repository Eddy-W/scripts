#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
 

import os
import sys
from datetime import date
from dateutil.rrule import rrule, DAILY
from dateutil.relativedelta import *
from optparse import OptionParser

import subprocess
import logging
import Queue
import threading
import time
import random
import os
import re
import types

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from UserDict import DictMixin


import saveXLS
 


def write_file(filename, content):
    fo = file(filename, 'w')
    fo.write(content)
    fo.close()

def logger_init(options, name):
    logger = logging.getLogger(name)
    #formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    formatter = logging.Formatter('%(threadName)-8s %(asctime)s %(levelname)-5s %(message)s')
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if (options.logfile is not None):
        file_handler = logging.FileHandler(options.logfile)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


class MyThread(threading.Thread):

    """A worker thread."""

    def __init__(self, q, logger, options):
        self._jobq = q
        self._logger = logger
        self._options = options
        threading.Thread.__init__(self)


    def run(self):
        """
........Get a job and process it.
........Stop when there's no more jobs
........"""
        while True:
            if self._jobq.qsize() > 0:
                sql = self._jobq.get()
                self._process_job(sql)
            else:
                break

    def _exec_cmd(self, cmd):
        self._logger.info( 'executing: ' + cmd )
        if not self._options.debug:
            p = subprocess.Popen(cmd,stdin = subprocess.PIPE,
                             stdout = subprocess.PIPE,
                             stderr = subprocess.STDOUT,
                             shell = True)

            log = p.communicate()[0]
            return p.returncode, log
        else:
            return 0, ''

    def _process_job(self, job):
        cmd = "/opt/hive/current/bin/hive -e \"" + job + "\""
        rt, log = self._exec_cmd(cmd)
        if rt==0:
            self._logger.info( cmd + ' done.' )
        else:
            self._logger.info( cmd + ' failed.')
            self._logger.info( log )

        time.sleep(random.random() * 3)


class odict(DictMixin):

    def __init__(self):
        self._keys = []
        self._data = {}

    def __setitem__(self, key, value):
        if key not in self._data:
            self._keys.append(key)
        self._data[key] = value

    def __getitem__(self, key):
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]
        self._keys.remove(key)

    def keys(self):
        return list(self._keys)

    def copy(self):
        copyDict = odict()
        copyDict._data = self._data.copy()
        copyDict._keys = self._keys[:]
        return copyDict

def write_file(filename, content):
    fo = file(filename, 'w')
    fo.write(content)
    fo.close()

def excute_sql(client, sql):
    success = False
    try_cnt = 0
    while not success:
        try:
            client.execute(sql)
            success = True
        except Thrift.TException, tx:
            try_cnt = try_cnt + 1
            print 'try: '+sql+' ,with '+str(try_cnt)+' num'
            print tx.message
            write_file('tmp/error.log', '%s' % (tx.message))
        except HiveServerException, tx:
            try_cnt = try_cnt + 1
            print 'try: '+sql+' ,with '+str(try_cnt)+' num'
            print tx.message
            write_file('tmp/error.log', '%s' % (tx.message))

def threads_alive(threads):
    for thread in threads:
        if thread.is_alive():
            return True
    return False

def exec_sqls(sqls,options,logger,jn):
	q = Queue.Queue(0)
	for sql in sqls:
		sql=('set mapred.job.name=%s; '+sql+';')%(jn)
		q.put(sql)
	if options.debug:
		for sql in sqls:
			print sql
		return

	threads = []
	for x in range(int(options.threads)):
		thread = MyThread(q, logger, options)
		thread.start()
		threads.append(thread)
		time.sleep(1)

	#if q is not empty, wait
	while threads_alive(threads):
		time.sleep(0.1)

def parse_args():
    usage = "usage: %prog [options]"
    parser = OptionParser()
    parser.add_option("--server", dest="server", default="10.28.5.134", help="hive server")
    parser.add_option("--port", dest="port", default="9000", help="hive server")
    parser.add_option("--threads", dest="threads", default=5,  help="thread number")
    parser.add_option("--logfile", dest="logfile", help="logfile name")
    parser.add_option("--debug", dest="debug", action="store_true", default=False, help="debug flag")
    # if (len(args) < 2):
    #     parser.print_help()
    #     sys.exit(1)
    (options, args) = parser.parse_args()

    # setup work directory
    work_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(work_dir)

    return options, args


PRODUCT='smartmedia'
 

def load_parameters():
	pars={}
	
	dt= sys.argv[1] 
	f= sys.argv[2] 
	cid= sys.argv[3] 
	domain= sys.argv[4] 
	taskid= sys.argv[5]
	output= sys.argv[6]
	dt_tmp=[]
	(dt1,dt2)=dt.split(',')
	if dt1==dt2:
		dt_tmp.append(dt1)
	else:
		dt_tmp.append(dt1)
		dt_tmp.append(dt2)
 
	pars['f']=f
	pars['cid']=cid
	pars['domain']=domain
	pars['taskid']=taskid
	pars['output']=output
	pars['date']=dt_tmp
	return pars
 
def load_create_sqls(pars):
	#set job name:   set mapred.job.name=foobarJob-123; 
	m_cid=pars['cid']
	m_taskid=pars['taskid']
	m_dt=pars['date']
	global PRODUCT 
	tmp_sqls=[]
	dd_str=''
	if len(m_dt)==1:
		dd_str="dd=\'"+m_dt[0]+"' "
	else:
		dd_str="dd>='"+m_dt[0]+"' and dd<='"+m_dt[1]+"' "
	table_str=['user_checkcookie_show_','user_checkcookie_click_','user_checkcookie_showns_','user_checkcookie_clickns_']
	table_name=['pb_showlog','pb_clicklog']
	tmp_table_raw=[]
	for i in (0,1):
		table_=table_str[i]+m_taskid
		tmpsql="""	 
		create table if not exists %s as 
		select sawLog_.rawLog_.ip_ ip,
			sawLog_.rawLog_.allyesid_ cookie,
			count(*) ns,
			sawLog_.vbannerid_ sid,
			sawLog_.regionid_ region,
			sawLog_.osid_ os,
			sawLog_.browserid_ browser,
			from_unixtime(sawLog_.rawLog_.timestamp_) time 
		from
			%s
		where
			product = '%s' and sawLog_.vbannerid_ = '%s' and %s
		group by sawLog_.rawLog_.ip_ , 
			 sawLog_.rawLog_.allyesid_ , 
			 sawLog_.vbannerid_ , 
			 sawLog_.regionid_ , 
			 sawLog_.osid_ , 
			 sawLog_.browserid_ , 
			 from_unixtime(sawLog_.rawLog_.timestamp_)
		"""%(table_,table_name[i],PRODUCT,m_cid,dd_str)
		tmp_sqls.append(tmpsql)
 
	for i in (2,3):
		table_=table_str[i]+m_taskid
		table_name_=table_name[i-2]
		tmpsql="""	 
		create table if not exists %s as select sawLog_.rawLog_.allyesid_ cookie, count(*) ns from
			%s
		where
			product = '%s' and sawLog_.vbannerid_ = '%s' and %s
		group by sawLog_.rawLog_.allyesid_
		order by ns desc
		"""%(table_,table_name_,PRODUCT,m_cid,dd_str)
		tmp_sqls.append(tmpsql)
	return  tmp_sqls
def load_drop_sqls(pars):
	m_cid=pars['cid']
	m_taskid=pars['taskid']
	m_dt=pars['date']
	global PRODUCT 
	tmp_sqls=[]
	dd_str=''
	if len(m_dt)==1:
		dd_str="dd=\'"+m_dt[0]+"' "
	else:
		dd_str="dd>='"+m_dt[0]+"' and dd<='"+m_dt[1]+"' "
	table_str=['user_checkcookie_show_','user_checkcookie_click_','user_checkcookie_showns_','user_checkcookie_clickns_']
	table_name=['pb_showlog','pb_clicklog']
	tmp_table_raw=[]
	for i in (0,1):
		table_=table_str[i]+m_taskid
		tmpsql="""	 
		drop table if  exists %s
		"""%(table_ )
		tmp_sqls.append(tmpsql)
 
	for i in (2,3):
		table_=table_str[i]+m_taskid
		table_name_=table_name[i-2]
		tmpsql="""	 
		drop table if   exists %s  
		"""%(table_ )
		tmp_sqls.append(tmpsql)
	return tmp_sqls
def load_query_sqls(pars):
	m_f=pars['f']
	m_taskid=pars['taskid']
	table_str=['user_checkcookie_show_','user_checkcookie_click_','user_checkcookie_showns_','user_checkcookie_clickns_']
	t_show=table_str[0]+m_taskid
	t_click=table_str[1]+m_taskid
	ns_show=table_str[2]+m_taskid
	ns_click=table_str[3]+m_taskid
	tmp_sqls={}
	tmp_sqls['1.显示cookie唯一数&总数']=['select count(distinct cookie),sum(ns) from %s'%t_show,2]
	tmp_sqls['1.显示ip唯一数&总数']=['select count(distinct ip),sum(ns) from %s'%t_show,2]
	
	tmp_sqls['1.点击cookie唯一数&总数']=['select count(distinct cookie),sum(ns) from %s'%t_click,2]
	tmp_sqls['1.点击ip唯一数&总数']=['select count(distinct ip),sum(ns) from %s'%t_click,2]
	
	
	tmp_sqls['1.一个cookie对应各3个不同ip数的个数']=["""
	select count(distinct b.cookie), count(c.cookie) from
	(select * from (select cookie,count(distinct ip) ns from %s group by cookie) a where ns=3 )b 
	join %s c on b.cookie = c.cookie
	 """%(t_show,t_show),2]
	tmp_sqls['1.一个cookie对应各4个不同ip数的个数']=["""
	select count(distinct b.cookie), count(c.cookie) from
	(select * from (select cookie,count(distinct ip) ns from %s group by cookie) a where ns=4 )b 
	join %s c on b.cookie = c.cookie
	 """%(t_show,t_show),2]
	tmp_sqls['1.一个cookie对应各大于4个不同ip数的个数']=["""
	select count(distinct b.cookie), count(c.cookie) from
	(select * from (select cookie,count(distinct ip) ns from %s group by cookie) a where ns>4 )b 
	join %s c on b.cookie = c.cookie  
	 """%(t_show,t_show),2]
	
	#return tmp_sqls
	
	tmp_sqls['2.显示点击重合']=["""
	select count(distinct b.cookie),sum(b.ns) from (select cookie from %s group by cookie) a 
	left outer join (select cookie,sum(ns) ns from %s group by cookie) b on 
	(a.cookie=b.cookie)
	"""%(t_show,t_click),2]
	 
	tmp_sqls['3.显示频次高的cookie所对应的点击数']=["""
	select count(a.cookie),sum(a.ns),count(b.cookie),sum(b.ns),b.ns from (select cookie,ns 
	from %s) a left outer join 
	(select cookie,ns from %s) b on (a.cookie=b.cookie) group by b.ns
	"""%(ns_show,ns_click),5]
	 

	tmp_sqls['3.点击频次高的cookie所对应的显示数']=["""
	select count(a.cookie),sum(a.ns),count(b.cookie),sum(b.ns),a.ns from (select cookie,ns 
	from %s) a left outer join 
	(select cookie,ns from %s) b on (a.cookie=b.cookie) group by a.ns
	"""%(ns_show,ns_click),5]
	
	#tmp_title='3.点示频次高于%s次的cookie所对应的点击数'%(m_f)
	#tmp_sqls[tmp_title]="""
	#select sum(ans),sum(asns),sum(bns),sum(bsns) from 
	#(select count(a.cookie) ans,sum(a.ns) asns,count(b.cookie) bns,sum(b.ns) bsns ,a.ns keyns from 
	#(select cookie,ns from %s) a left outer join 
	#(select cookie,ns from %s) b on (a.cookie=b.cookie) group by a.ns) 
	#b where keyns>%s
	#"""%(ns_show,ns_click,m_f)
	
	tmp_sqls['11.一个cookie对应多个不同ip数列表']=["""
	select   b.cookie,b.ns, count(c.cookie) from
	(select * from (select cookie,count(distinct ip) ns from %s group by cookie) a where ns>=2 )b 
	join %s c on b.cookie = c.cookie group by b.cookie, b.ns order by ns 
	 """%(t_show,t_show),3]
	 
	tmp_sqls['12.无显示cookie']=["""
	 select b.cookie,b.ip,b.ns from (select cookie from %s group by cookie) a 
	 right outer join (select cookie,ip,sum(ns) ns from %s group by cookie,ip) b on 
	 (a.cookie=b.cookie) where a.cookie is null  
	 """%(t_show,t_click),3]
	
	tmp_sqls['13.超过频次设定的显示cookie']=["""
	select cookie,ns from %s where ns>%s
	"""%(ns_show,m_f),2]
	
	m_dt=pars['date']
	dd_str=''
	if len(m_dt)==1:
		dd_str="dd=\'"+m_dt[0]+"' "
	else:
		dd_str="dd>='"+m_dt[0]+"' and dd<='"+m_dt[1]+"' "
	m_cid=pars['cid']
	global PRODUCT 
	table_name=['pb_showlog','pb_clicklog']
	tmp_sqls['4.refer显示数&点击数']=["""
	select a.refer,a.ns,b.ns from (select sawlog_.rawlog_.referrer_ refer,count(*) ns from %s where 
	product='%s' and (sawLog_.vbannerid_='%s')  
	and %s group by sawlog_.rawlog_.referrer_) a full outer join (select sawlog_.rawlog_.referrer_ refer,
	count(*) ns from %s where product='%s' and (sawLog_.vbannerid_='%s')  
	and %s group by sawlog_.rawlog_.referrer_) b on (a.refer=b.refer) order by a.ns desc
	"""%(table_name[0],PRODUCT,m_cid,dd_str,table_name[1],PRODUCT,m_cid,dd_str),3]
	

	
	return  tmp_sqls

def excute_querysql_byfile(res, sqls, options, LOG,pars,jn):
	m_taskid=pars['taskid']

	sql_pre="insert overwrite   directory '/user/anson/checkcookietmp/%s' "%(m_taskid)
	for k,v in sqls.items(): 
		if res.has_key(k)==False or (res.has_key(k)==True and len(res[k])==0):
			print 'First-Run:',k
			vv=v[0]
			cmd_sql=sql_pre+vv
			sql_tmp_=[]
			sql_tmp_.append(cmd_sql)
			fail_flag=-1
			try:
				exec_sqls(sql_tmp_, options, LOG,jn)
			except:
				fail_flag=1
				pass
			if fail_flag==1:
				res[k]=-1
			else:
				cmd="hadoop fs -text /user/anson/checkcookietmp/%s/* > /home/www/allyes/da/tmp/checkcookie-%s/queryres" %(m_taskid,m_taskid)
				os.system(cmd)
				fin=open('tmp/checkcookie-%s/queryres'%(m_taskid))
				res_tmp_=[]
				for line in fin.readlines():
					line=line[:-1].replace('\x01','\t')
					line=line.replace('\\N','NULL')
					if len(line.split('\t'))==v[1]:
						res_tmp_.append(line)
				res[k]=res_tmp_
	
	#outf=open('/home/www/allyes/da/tmp/checkcookie-%s/out.txt'%(m_taskid),'w')
	#for key in res:
	#	outf.write(key)
	#	for key2 in res[key]:
	#		outf.write('\t')
	#		outf.write(key2+'\t')
	#		outf.write('\t'.join(res[key][key2] ))
	#		outf.write('\n')
	   
	   
	cmd="hadoop fs -rmr /user/anson/checkcookietmp/%s "%(m_taskid)
	os.system(cmd)
		
	return res
def checkRes(res):
	for k,v in res.items():
		if v==-1:
			return False
	return True

def main():
	options, args = parse_args()
	pars=load_parameters()
	create_sqls=load_create_sqls(pars)
	query_sqls=load_query_sqls(pars)
	drop_sqls=load_drop_sqls(pars)
	LOG = logger_init(options, 'Main')
	jn='checkcookie-%s'%pars['taskid']
     

	try:
		#sql="insert overwrite   directory '/user/anson/checkcookietmp' select * from user_wangwentao_mediamax_media"
		#sqls=[]
		#sqls.append(sql)
		#print sqls
		#exec_sqls(sqls, options, LOG)
		#cmd="hadoop fs -text /user/anson/checkcookietmp/* > /home/www/allyes/da/mediatmp" 
		#print cmd
		#os.system(cmd)
		print "###############################CREATE_SQLS###############################"
	 
		exec_sqls(create_sqls, options, LOG,jn)
		#tmp_sqls={}
		#tmp_sqls['测试']= "select * from user_wangwentao_mediamax_media limit 5"
		res={}
		print "###############################QUERY_SQLS###############################"
		res=excute_querysql_byfile(res,query_sqls, options, LOG,pars,jn)
		retry_cnt=3
		for i in range(0,retry_cnt):
			if checkRes(res)==True:
				print "###############################CHECK_QUERY_SQLS OK!###############################"
				break;
			else:
				print "###############################RETRY_QUERY_SQLS###############################"
				#res=excute_querysql_byfile(res,query_sqls, options, LOG,pars,jn)
				
		print "###############################RES###############################"	
		for k,v in res.items():
			print k+": ",len(v)

		err=saveXLS.saveRes(res,pars)
		print err
		print "###############################DROP_SQLS###############################"
		#exec_sqls(drop_sqls, options, LOG,jn) 
	except Exception,e:
		 print e
 



if __name__ == "__main__":
	main()
