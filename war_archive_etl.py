
import sys, os
import pandas as pd
import numpy as np
import scipy as scp
import sqlalchemy

import re
import requests, urllib3
from lxml import html
import io
import zipfile
import time
from multiprocessing.pool import ThreadPool

from ref_war_data_dicts import ref_war_daily_bat_cols, ref_war_daily_pitch_cols

import pandas_profiling

###
# This script is responsible for the ETL of WAR daily files (bat and pitch)
# It also should have the looping ability to load many data files sequentially

########
#
#   CONFIGURABLES
war_ref_site = 'https://www.baseball-reference.com/data/'
war_zip_dir = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\zip_data'
war_extract_dir = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\extracted_data'
sample_file_bat = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\war_daily_bat_sample.txt'
sample_file_pitch = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\war_daily_pitch_sample.txt'

#########
#   For now, requests-based versions of web pull
#
def download_war_archives(limit_num_zips = 10):
    #write a file from a particular url, using the war_zip_dir as save location
    def url_response(url, pool_manager):
        #req = requests.get(url, stream = True)
        req = pool_manager.urlopen('GET', url)
        zip_file = req.read()
        
        write_path = os.path.join(war_zip_dir, url.replace(war_ref_site, '')) 
        with open(write_path, 'wb') as f:
            f.write(zip_file)
        #z = zipfile.ZipFile(io.StringIO(str(req.content)))
        #z.extractall(write_path)

    #get list of urls
    page = requests.get(war_ref_site)
    webpage = html.fromstring(page.content)
    file_list = webpage.xpath('//a/@href')
    pool_manager = urllib3.PoolManager()

    index = 0 
    for zip_file in file_list:
        if(re.match('war_archive-20*', zip_file)):
            print("Downloading", zip_file, "to", war_zip_dir)
            url_response(war_ref_site + zip_file, pool_manager)
            time.sleep(2)
            if index > limit_num_zips:
                print ("Maximum download limit reached. Returning...")
                return 
        else:
            print("Skipped over", zip_file)
            continue
        

def refresh_war_archives():
    raise NotImplementedError


########
# Taking in a file name, return a pandas dataframe from raw csv
#
def extract_single_war_csv(file_name, ref_cols, year_only = 2019):
    with open(file_name) as f:
        stats_df = pd.read_csv(f, sep = ',')
    #Filter to year
    stats_df = stats_df[stats_df.year_ID == year_only]

    #checks on 1) num cols, 2) num rows
    print("Num player entries = ", stats_df.shape[0])
    print("Num columns = ", stats_df.shape[1])

    #check if cols agree with ref dict
    columns_read = stats_df.columns.tolist()

    if( set(columns_read) != set(ref_cols)):
        print("Column mismatch - exiting")
        print("Columns mismatched = ", set(columns_read) - set(ref_cols))
        return None
    else:
        return stats_df

#######
#
#
def transform_war_pitch(stats_df, remove_all_but_recent_year):
    raise NotImplementedError
def transform_war_bat(stats_df, remove_all_but_recent_year):
    raise NotImplementedError


#######
#
#
def load_war_pitch():
    raise NotImplementedError
def load_war_bat():
    raise NotImplementedError



