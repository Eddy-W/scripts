#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
import os
import pwd
import re
import shutil
import subprocess
import sys
import urllib
import urllib2
#import lxml.etree

def enum(**enums):
    return type('Enum', (), enums)

Status = enum(STARTING=1,RUNNING=2,DONE=3,FAILED=4,STOPPING=5,STOPPED=6)

def load_spec(file):
    kvp = re.compile(r'(.*)\s*=\s*(.*)');
    spec = {}
    fp = open(file, 'r')
    for line in fp:
	if line[0] == '#':
	    # comment line
	    continue
    	m = kvp.match(line.rstrip())
	if m:
	    spec[m.group(1)] = m.group(2)
    fp.close()
    return spec

def check_spec(spec, *args):
    failed = False
    for k in args:
	if k not in spec:
	    print >> sys.stderr, '参数' + k + '没有设置'
	    failed = True
    return failed

def check_spec_unique(spec, *args):
    i = 0
    param = {}
    for k in args:
        if k in spec:
            param[i] = k
            i += 1
    if i == 1:
        param[-1] = False
        return param
    outmsg = '参数'
    if i == 0:
        for k in args:
            outmsg += ' ' + k
        outmsg += ' 需要设置其中一个'
    elif i > 1:
        for k in args:
            if k in spec: 
	        outmsg = outmsg + ' ' + k
        outmsg += ' 不能同时设置'	
    print >> sys.stderr, outmsg
    param[-1] = True
    return param;

def update_status(spec, code, msg=None):
    if msg is None:
    	msg = ''
    qs = { 'taskid': spec['taskid'], 'status': code, 'msg': msg }
    try:
    	urllib2.urlopen(spec['callback'], urllib.urlencode(qs)).close()
    except:
    	print urllib.urlencode(qs)	# what else?
    	pass
def parse_value(strx):
	 
	strx=strx[:strx.find('</value>')]
	strx=strx[strx.find('<value>')+7:]
	return strx
 

def kill_jobs(jobname):
   
	jobs = []
	try:
		user = pwd.getpwuid(os.getuid())[0]
		p = subprocess.Popen(['hadoop', 'job', '-list'],stdout=subprocess.PIPE).stdout
		for line in p:
			flds = line.rstrip().split('\t')
			if len(flds) == 6 and flds[0][:4] == 'job_' and flds[3] == user:
				jobs.append(flds[0])
		p.close()
	except:
		pass
	 
	xmls = {}
	for j in jobs:
		try:
			p = subprocess.Popen(['hadoop', 'job', '-status', j],stdout=subprocess.PIPE).stdout
			for line in p:
				if line[:6] == 'file: ':
					xmls[j] = line.rstrip()[6:]
			p.close()
		except:
			pass
	print xmls
	jobs = [];
	for j,f in xmls.items():
		try:
			p = subprocess.Popen(['hadoop', 'fs', '-text', f],stdout=subprocess.PIPE).stdout
 
			for line in p.readlines():
				if line.find('mapred.job.name')>=0:
					print parse_value(line)
					if jobname==parse_value(line):
						jobs.append(j)
			p.close()
		except:
			pass

	for j in jobs:
		subprocess.call(['hadoop', 'job', '-kill', j])
		print 'kill',j

def dfs_rmdir(dir):
    try:
	subprocess.call(['hadoop', 'fs', '-rmr', dir])
    except:
    	pass
 
def main():
	if len(sys.argv) < 3:
		print >> sys.stderr, 'Usage: ' + sys.argv[0] + ' {start|stop|list} taskspec'
		return 64
	cmd = sys.argv[1]
	bindir = os.path.dirname(os.path.abspath(__file__))
	print bindir
	spec = load_spec(sys.argv[2])
	if check_spec(spec, 'taskid', 'callback'):
		return 65
	taskid = spec['taskid']
	if not taskid.isdigit():
		# check for illegal taskid because we'll be using it for dir names
		print >> sys.stderr, 'taskid必须全是数字'
		return 65
	tasktype = spec['tasktype']
	# print >> sys.stderr, tasktype
	tmpdir = bindir+'/tmp/checkcookie-' + taskid
	dfsdir = 'checkcookie-' + taskid
	pidfile = os.path.join(tmpdir, 'pid')
	outfile = os.path.join(tmpdir, 'out')
	debug = 'debug' in spec and spec['debug'] or os.getenv('SMADEBUG','0')
	debug = int(debug)
	if cmd == 'start':
		if check_spec(spec, 'output'):
			return 65
		if os.path.isfile(pidfile):
			# task already running?
			print pidfile
			print 'running'
			pass
		else:
			# TODO: close the race window?
			# update_status(spec, Status.RUNNING, '开始运行')
			os.path.isdir(tmpdir) or os.mkdir(tmpdir)
			path = os.path.join(bindir, 'checkCookie_main.sh')
 
			pid = subprocess.Popen(['/bin/sh', path, sys.argv[2],tmpdir, dfsdir, tasktype, spec['output'],taskid],
			preexec_fn=os.setsid, cwd=tmpdir,
			stdin=open(os.devnull,'r'), stdout=open(outfile,'w'),
			stderr=subprocess.STDOUT, close_fds=True).pid
			open(pidfile,'w').write(str(pid))
		update_status(spec, Status.RUNNING, '开始运行')
	elif cmd == 'test':
		print "###############################TEST###############################"
		 
		kill_jobs(dfsdir)
		 
	elif cmd == 'stop':
		print "###############################KILL JOBS###############################"
		if os.path.isfile(pidfile):
			try:
				pid = int(open(pidfile,'r').read())
				os.killpg(pid, 15)
			except:
				pass
				os.unlink(pidfile)
		else:
			# task not running?
			pass
		#kill_jobs(dfsdir)  #考虑是否添加？
		os.unlink(pidfile)
		#if debug == 0:
			#dfs_rmdir(dfsdir)
			#shutil.rmtree(tmpdir, ignore_errors=True)
		update_status(spec, Status.STOPPED, '运行终止');
 
	elif cmd == 'done':
		if len(sys.argv) < 4:
			return 64
		if os.path.isfile(pidfile):
			os.unlink(pidfile)
		#kill_jobs(dfsdir + '/')
		#if debug == 0:
			#dfs_rmdir(dfsdir)
			#shutil.rmtree(tmpdir, ignore_errors=True)
		update_status(spec, sys.argv[3], sys.argv[4])
	return 0

if __name__ == "__main__":
    if 'SUDO_USER' in os.environ:
		del os.environ['SUDO_USER']
		sys.argv[0] = os.path.join(os.path.dirname(__file__),'checkCookie.sh')
		os.execv(sys.argv[0], sys.argv)
    sys.exit(main())
