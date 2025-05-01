#src\main.py

"""

Main
====


History
----------

20250501 -- Add simple Logging

"""


import logging
import os
from dotenv import load_dotenv

load_dotenv() 


LOGDIR = os.getenv('LOGDIR')
LOGFILE = "etl.log"
LOGPATH = LOGDIR + LOGFILE

logging.basicConfig(
    level=logging.INFO,
    filename=LOGPATH,
    encoding='utf-8', 
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)    

