#!/usr/bin/python

import os
import subprocess

def exec_cmd(cmd, print_flag=True):
	if print_flag == True :
        	print cmd
	return os.system(cmd)

#return cmd process returncode
def execute(cmd, print_flag=True, logfile='', fail_exit=False):
	if print_flag == True :
        	print 'execute:' + cmd

    	if(logfile == ''):
        	fp_log = None
		outHandler = subprocess.PIPE
    	else:
        	fp_log = open(logfile, 'a+')
		outHandler = fp_log

	p = subprocess.Popen(cmd,stdin = subprocess.PIPE, \
                         stdout = outHandler,
                         stderr = subprocess.STDOUT,
                         shell = True)
	

	retcode = p.wait()
	if(not fp_log is None):
        	fp_log.flush()
        	fp_log.close()

	if (fail_exit):
        	if (retcode!=0):
            		print 'Command failed: ' + cmd
            		print 'Return error code: ' + str(returncode)
            		sys.exit(p.returncode)

	#print '-----retcode:' + str(retcode) 
	return retcode

