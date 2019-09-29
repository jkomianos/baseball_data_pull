
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
war_extract_dir = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\extracted_data'
sample_file_bat = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\war_daily_bat_sample.txt'
sample_file_pitch = r'C:\Users\jkomi\Code\baseball_data_pull\war_data\war_daily_pitch_sample.txt'

#########
#   For now, requests-based versions of web pull
#
def download_war_archives(limit_num_zips = 5):
    #write a file from a particular url, using the war_extract_dir as save location
    def url_zip_download(url, zip_file_name):
        req = requests.get(url)
        zip_file = zipfile.ZipFile(io.BytesIO(req.content))

        #make the dir if it doesnt exist already
        save_dir = os.path.join(war_extract_dir, zip_file_name.replace('.zip',''))
        if (not os.path.isdir(save_dir)):
            os.mkdir(save_dir)
        zip_file.extractall(save_dir)  

        #req = pool_manager.urlopen('GET', url)
        #zip_file = req.read()
        #print(zip_file)
        #write_path = os.path.join(war_zip_dir, url.replace(war_ref_site, '')) 
        #with open(write_path, 'wb') as f:
        #    f.write(zip_file)

    #get list of urls
    page = requests.get(war_ref_site)
    webpage = html.fromstring(page.content)
    file_list = webpage.xpath('//a/@href')
    pool_manager = None #urllib3.PoolManager()

    index = 0 
    for zip_file in file_list:
        if(re.match('war_archive-20*', zip_file)):
            print("Extracting", zip_file, "to", war_extract_dir)
            url_zip_download(war_ref_site + zip_file, zip_file)
            time.sleep(2)
            if index > limit_num_zips:
                print ("Maximum download limit reached. Returning...")
                return 
        else:
            print("Skipped over", zip_file)
            continue

### Would be nice...
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

def extract_all_war_csv(file_name, ref_cols, data_dir):

    all_stats_df = pd.DataFrame()

    for date_dir in os.listdir(data_dir):
        #get the date from directory
        date_pattern = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
        year_pattern = '[0-9]{4}'
        date_match = re.search(date_pattern, date_dir) 
        year_match = re.search(year_pattern, date_dir)
        try:
            date_str = date_match.group()
            year = int(year_match.group())
        except:
            continue

        #get the daily df, add a date column, get only this year's data
        stats_df = extract_single_war_csv(os.path.join(date_dir, file_name), ref_cols, year_only = year)
        stats_df['daily_date'] = pd.to_datetime(date_str)

        if(all_stats_df.shape[0] == 0):
            all_stats_df = stats_df
        else:
            all_stats_df = all_stats_df.append(stats_df)

        return all_stats_df
#######
#
#
def transform_war_pitch(stats_df):
    raise NotImplementedError
def transform_war_bat(stats_df):
    raise NotImplementedError
def load_war_pitch(stats_df):
    raise NotImplementedError
def load_war_bat(stats_df):
    raise NotImplementedError

def main():
    download_war_archives()
    unzip_war_archives()

if __name__ == '__main__':
    main()