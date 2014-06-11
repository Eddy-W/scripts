#!/usr/bin/python

import os
import time
from datetime import datetime, date, timedelta
from common.command import execute


#input params: curr_date is a date string like "YYYY-MM-DD"
def before_date(curr_date, before_days_num):
    result_date = datetime.strptime(curr_date, '%Y-%m-%d') - timedelta(before_days_num)
    result_date = result_date.strftime("%Y-%m-%d")
    return result_date

def after_date(curr_date, after_days_num):
    result_date = datetime.strptime(curr_date, '%Y-%m-%d') + timedelta(after_days_num)
    result_date = result_date.strftime("%Y-%m-%d")
    return result_date

def strtodatetime(datestr,format):
    return datetime.strptime(datestr,format)

def datediff(beginDate,endDate):
    format="%Y-%m-%d";
    begd = strtodatetime(beginDate,format)
    endd = strtodatetime(endDate,format)
    oneday = timedelta(days=1)
    count=0
    while begd!=endd:
        endd = endd-oneday
        count+=1
    return count

def makedir_hdfs(hdfs_dir, rm_flag=True) :
        execute('hadoop fs -mkdir %s  > /dev/null 2>&1' % hdfs_dir, True)
        if rm_flag == True :
                execute('hadoop fs -rm --skipTrash %s/*  > /dev/null 2>&1' % hdfs_dir, False)

def check_hdfsdir_exist(hdfs_dir) :
    cmd = "hadoop fs -test -d %s > /dev/null 2>&1" % hdfs_dir
    retcode = execute(cmd, False)
    ret = True if (retcode == 0) else False
    return ret


def is_hdfsdir_empty(hdfs_dir):
        if not check_hdfsdir_exist(hdfs_dir) :
                return True

        cmd = "hadoop fs -dus %s | awk '{print $2}'" % (hdfs_dir)
#       print cmd
        dir_length = commands.getoutput(cmd)
        if dir_length == '' :
                dir_length = '0'

        dirlen = int(dir_length)
        ret = False if (dirlen > 0) else True
        return ret


