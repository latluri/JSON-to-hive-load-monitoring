import os
import sys
import re
#import pandas as pd
import commands
import ast
import itertools
import pyspark.sql.functions
from pyspark.sql.functions import col
from pyspark.sql.functions import current_date
from datetime import datetime,timedelta
from collections import Counter
import re
#import numpy as np


from pyspark import SparkContext, SparkConf, HiveContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlContext = HiveContext(sparkContext=sc)
sqlCtx = HiveContext(sparkContext=sc)

def freq(lst):
    d = {}
    for i in lst:
        if d.get(i):
            d[i] += 1
        else:
            d[i] = 1
    return d


def get_nested_keys(a):
    key_list=[]
    for i in a.keys():
        b=a[i]
        if "{" in str(b):
                key_list=key_list+[i]
    return(key_list)

date_to_investigate=datetime.strptime(sys.argv[1], '%Y-%m-%d')
print(date_to_investigate)
today_=datetime.today().strftime('%Y-%m-%d')
days_to_go_back=abs((today_ - date_to_investigate).days)
print(days_to_go_back)


sys.exit(0)
