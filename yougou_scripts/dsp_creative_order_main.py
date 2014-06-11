#!/usr/bin/python
#@writer:Deng zhangming 

import os
import sys, commands
import string
import logging
import time

import ConfigParser
from datetime import datetime, date, timedelta
from optparse import OptionParser
#import pyhdfs  
from common.xmlParser import XmlParser
from common.command import execute,exec_cmd
from common.utils import before_date
from common.utils import makedir_hdfs, check_hdfsdir_exist, is_hdfsdir_empty



#export CLASSPATH=/home/tonydeng/hadoop-0.20.2-cdh3u5
#export HADOOP_CONF_DIR=${hadoopconfdir}

g_homedir = ''
g_hadoopHome = 'hadoop'
logger = logging.getLogger('dspCOP')
logger.setLevel(logging.DEBUG)

#===========some configuration params

#=================================

today_date = date.today()
yestoday_date = today_date - timedelta(days=1)
beforeyestoday_date = today_date - timedelta(days=2)


class ConfigParams :
    def __init__(self):
        self.configParser = XmlParser()

    def initialize(self, xmlConfigFile):
        self.parseXmlConfigParams(xmlConfigFile)

    def parseXmlConfigParams(self, xmlConfigFile) :
        self.local_tempout_dir = self.configParser.get_node_value('system/WorkLocalDir')
        self.advertiser_name = self.configParser.get_node_prop('advertisers/advertiser', 'name')
        self.track_product = self.configParser.get_node_value('advertisers/advertiser/track_product')
        self.track_db = self.configParser.get_node_value('advertisers/advertiser/track_db')
        self.sitecode = self.configParser.get_node_value_by_index('advertisers/advertiser/sitecodes/sitecode', 0)



def run_puv_task():
        logger.info('####Running task of query puv...')
        cmd = " sh *.sh params" \
                % ("conf.xml")

	logger.info('--execute:' + cmd)
        retcode = execute(cmd)

	return retcode

def main():
	ret = 1
	if options.action_type == '1' or options.action_type == 'puv' : 
		ret = run_puv_task()
        elif options.action_type == 'cpm':
		ret = run_cpm_task()

	return ret

	
def initialize_args():
    	g_homedir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.')
        print 'homedir:' + g_homedir

        usage = "usage: %prog [options] arg"
    	parser = OptionParser(usage)
    	parser.add_option("-a", "--action",
                        dest="action_type", default='all',
                        help="aciton type: 1/puv, 2/exchangepv, 3/cpm, 4/orderparse, 5/mediaoverlap")
    	parser.add_option("-d", "--db", default='ifc',
                        dest="db",
                        help="tracklog db")
    	parser.add_option("-t", "--date",
                        dest="date",
                        help="log date")
    	parser.add_option("-c", "--conf",  default='',
                        dest="configFile",
                        help="advertiser id")


    	(options, args) = parser.parse_args()

	if not options.action_type :
		options.action_type = 'all'

	if not options.configFile :
		options.configFile = "conf.xml"

	if not options.date  :
		print 'please input date.'
                parser.print_help()
                return None

        if (options.date.strip() == 'today') :
                options.date = str(today_date)
        elif (options.date.strip() == 'yestoday') :
                options.date = str(yestoday_date)
        elif (options.date.strip() == 'beforeyestoday') :
                options.date = str(beforeyestoday_date)


	return options


def init_logger(options):
	logfile = './logs/cop_run_%s.log' % options.date 
	fh = logging.FileHandler(logfile)
	fh.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)

	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	fh.setFormatter(formatter)
	ch.setFormatter(formatter)

	logger.addHandler(fh)
	logger.addHandler(ch)


if __name__ == "__main__":
    options = initialize_args()
    if options is None:
	exit(1)

    execute('mkdir -p ./logs > /dev/null 2>&1')
    init_logger(options)
    
    mr_logfile = './logs/mr_%s.log' % options.date


    logger.info("----------------work flows running ----------------")
    print 'db          :' + options.db
    print 'date        :' + options.date
    print 'config file :' + options.configFile

    ret = main()
    logger.info("----------------------ok---------------------------------\n\n")
    exit(ret)
	


