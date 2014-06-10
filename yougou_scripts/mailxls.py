#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
 
import os
import sys
from datetime import datetime
from datetime import date
from dateutil.rrule import rrule, DAILY
from dateutil.relativedelta import *
from datetime import timedelta
import codecs

import subprocess
import logging
import Queue
import threading
import time
import random
from optparse import OptionParser
 
def send_mail(options,dt, dd, attach_file, LOG):
    subject = "[%s]yougou_dsp_performance" % dd

    body_content = u"\n\nDear all\n\n"
    body_content += (u"附件为%s优购DSP报表, 有任何疑问请及时和我联系。\n" % dd)
    body_content += (u"此邮件为系统自动发送邮件，请勿回复，谢谢!\n")
    body_content += (u"\n\n谢谢\n王文涛\n")

    recipients = "xiaolu_yan@allyes.com,sarah_yong@allyes.com"
    cc_recipients = "hu_jia@allyes.com,elan_gu@allyes.com,tony_deng@allyes.com,brian_xu@allyes.com,wentao_wang@allyes.com"

    send_raw_mail(options, LOG, subject, body_content, recipients, cc_recipients, attach_file)
def write_file(filename, content, mode='w', encoding='utf-8'):
    fo = codecs.open(filename, mode, encoding)
    fo.write(content)
    fo.close()
def send_raw_mail(options, LOG, subject, body_content, recipients, cc_recipients, attach_file):

    body_filename = "mail_body.txt"
    write_file(body_filename, body_content)
    time.sleep(1)
    

    cmd = "mail_exchange -s '%s' -f '%s' -r '%s' --cc_recipients '%s' -a '%s' -u 'wentao_wang' -p '2238681Xw' "\
    % (subject, body_filename, recipients, cc_recipients, attach_file)
    print cmd
    rt, log = exec_cmd(cmd, LOG)
    if rt != 0:
	    LOG.error("mail send fail at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n" + log)
	    sys.exit(1)
    cmd = "rm -f " + body_filename
    rt = os.system(cmd)
    if rt != 0:
       LOG.error("rm file fail, " + body_filename)

def exec_cmd(cmd, LOG):
    LOG.info( 'executing: ' + cmd )
    p = subprocess.Popen(cmd,stdin = subprocess.PIPE,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.STDOUT,
                         shell = True)

    log = p.communicate()[0]
    return p.returncode, log
def parse_args():
    usage = "usage: %prog [options]"
    parser = OptionParser()
    parser.add_option("--logfile", dest="logfile", help="logfile name")
    parser.add_option("--loglevel", dest="loglevel", help="logfile level", default="debug")
    parser.add_option("--debug", dest="debug", action="store_true", default=False, help="debug flag")
    (options, args) = parser.parse_args()

    return options, args
       
def logger_init(options, name):
    logger = logging.getLogger(name)
    #formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    formatter = logging.Formatter('%(name)-10s %(asctime)s %(levelname)-5s %(message)s')
    if options.loglevel == 'debug':
        logger.setLevel(logging.DEBUG)
    elif options.loglevel == 'info':
        logger.setLevel(logging.INFO)
    elif options.loglevel == 'warn':
        logger.setLevel(logging.WARN)
    elif options.loglevel == 'error':
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if (options.logfile is not None):
        file_handler = logging.FileHandler(options.logfile)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger

def main():
	options, args = parse_args()
	LOG = logger_init(options, 'Main')
	fp=sys.argv[1]
	dt=sys.argv[2]
	dd=sys.argv[3]
	output_filename = r'%s/yougou_dps_report_%s.xls'%(fp,dt)
	print LOG
	print options
	print args
	print output_filename
	send_mail(options, dt,dd, output_filename, LOG)

if __name__ == '__main__':
    main()
    #mail_exchange -s '[1031]_Adsvana_model_daily_data'  -f 'mail_body.txt' -r 'wentao_wang@allyes.com'   -a 'res-2013-10-31/output-1' -u 'wentao_wang' -p'2238681Xww'
