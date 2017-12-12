
import sys, os, json
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

reload(sys)
sys.setdefaultencoding("utf-8")

sys.path.insert(0, '.venv/lib/python2.7')
sys.path.insert(0, '.venv/lib/python2.7/site-packages')

os.environ['PATH'] = '%s:%s'%(os.environ['PATH'], '.')

from main import *
