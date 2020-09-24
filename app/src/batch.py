import pyspark
from pyspark import SparkContext
import pandas as pd
import numpy as np
import requests
from requests import get
import datetime
from http import HTTPStatus
import json
from json import dumps
from calendar import monthrange
from time import strftime
import os
import csv
import time
from pathlib import Path
from typing import Iterable, Dict, Union, List
import pyarrow as pa
import redis
import urllib
import sys
import multiprocessing

# Batch process to load data from API into dataframes and save to redis


# Import our dataframes class

from dataframes import CasesData

# Contexts, & redis connections
sc = SparkContext()
redis_connection = redis.Redis(host="localhost", port=6379, db=0)
arrow_context = pa.default_serialization_context()

StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]



# Mapping of API Hierarchy levels to app Hierachy levels
LEVELS_DICT = {"nation" : "Nation", "region" : "Region", "utla" : "Upper tier local authority", 
                "ltla" : "Lower tier local authority" } #, "nhsRegion" : "NHS Region" }

#LEVELS_DICT = {"nation" : "Nation"} 

# Mappings of API attributes to dataframe column names
COLUMN_RENAMES = {"name" : "Area name", "code" : "Area code", "type" : "Area type", "HospitalCases": "Hospital Cases", "Deaths28" : "Deaths within 28 Days of Positive Test"}

CASES_THRESHOLD = 250000    # Minimum number of cases to have loaded as a sanity check

EMPTY_DF = pd.DataFrame(columns=['Date','Area name','Area code','Area type','Cases','Tests','Hospital Cases','Deaths within 28 Days of Positive Test'])


def get_api_paginated_dataset(filters: FiltersType, structure: StructureType) -> APIResponseType:
    """ Extracts paginated data by requesting all of the pages and combining the results.
    
    Lifted from https://coronavirus.data.gov.uk/developers-guide
    Parameters:
        filters: Iterable[str]  - API filters. See the API documentation
        structure: Dict[str, Union[dict, str]]  - Structure parameter. See API documentation

    Returns
        Union[List[StructureType], str]
            Comprehensive list of dictionaries containing all the data for given ``filters`` and ``structure``.
    """

    endpoint = "https://api.coronavirus.data.gov.uk/v1/data"

    api_params = {
        "filters": str.join(";", filters),
        "structure": dumps(structure, separators=(",", ":")),
        "format": "json" 
    }

    #print(api_params)

    data = list()
    page_number = 1

    while True:
        # Adding page number to query params
        api_params["page"] = page_number
        
        response = get(endpoint, params=api_params, timeout=120)

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            #raise RuntimeError(f'Request failed: {response.text}')
            print (f'API Request failed: {response.text}')
            data = list()
            break
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break

        current_data = response.json()
        page_data: List[StructureType] = current_data['data']
        
        data.extend(page_data)

        # The "next" attribute in "pagination" will be `None` when we reach the end.
        if current_data["pagination"]["next"] is None:
            break

        page_number += 1

    return data


def get_structure(level):
    
    """ Returns API query structure for a given level """  

    query_structure = {
        "Date": "date",
        "name": "areaName",
        "code": "areaCode",
        "type": "areaType",
        "Cases": "newCasesBySpecimenDate",
        "Tests": "newTestsByPublishDate",
        "HospitalCases": "hospitalCases",
        "Deaths28": "newDeaths28DaysByPublishDate"
    }

    if level == "nation":
        query_structure["Cases"] = "newCasesByPublishDate"

    return query_structure     


def get_api_dataframe(level, month):
    """ Gets API data for a given area hierachy level and month (yyyy-mm) and saves it as a dataframe to Redis """     

    query_filters = [
        "areaType="+level,
        "date>="+month+"-01",
        "date<="+month+"-31"
    ]
    
    # Query API data for this hierachy level  (API can be flaky so try up to 3 times)
    attempts = 1
    json_data = list()
    while attempts < 4 and len(json_data) == 0:
        print(f"Querying API..... Attempt {attempts} - Level {level} - Month {month}")
        json_data = get_api_paginated_dataset(query_filters, get_structure(level))
        attempts += 1 
        if len(json_data) == 0:
            # Wait 30 seconds before trying again
            time.sleep(30)

    checktotal = None
    if len(json_data) > 0:

        # Convert json data to dataframe
        df = pd.DataFrame.from_dict(json_data, orient='columns')
       
        # Rename api level with our application level text (e.g. utla to Upper tier local authority)
        df['type'] = df['type'].replace(level,LEVELS_DICT[level])

        # Tidy up dataframe, rename columns, replace some data, fill nas, sort, convert types etc
        df.rename(columns=COLUMN_RENAMES,inplace=True)
        df['Date'] = pd.to_datetime(df['Date'],format="%Y-%m-%d")
        df.sort_values(by=['Date'], inplace=True)
        df.set_index('Date',inplace=True)
        df.fillna(0,inplace=True)
        df = df.astype({'Cases': int, 'Tests': int, 'Hospital Cases': int, 'Deaths within 28 Days of Positive Test': int})

        # Save dataframe to redis - note we create a new connection here so it can be serialised by Spark
        r = redis.Redis(host="localhost", port=6379, db=0)
        ac = pa.default_serialization_context()

        # Save dataframe to redis - first backup previous data to "Old.xxx" key
        oldkey = "Old.Cases."+level+"."+month
        currentkey = "Cases."+level+"."+month

        if r.exists(oldkey):
            r.delete(oldkey)
        
        if r.exists(currentkey):
            r.rename(currentkey,oldkey)

        # Save new data
        r.set(currentkey, ac.serialize(df).to_buffer().to_pybytes())

        checktotal = df['Cases'].sum()

    print(f"Total Cases : {checktotal}")
    return checktotal



def get_api_last_modified():

    print("Checking API Data Status....")
    url = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=nation;areaName=england&structure=%7B%22name%22:%22areaName%22%7D'
    response = requests.get(url, timeout=60)
    print("API Response code",response.status_code)

    if response.status_code == 200:    
        #print(response.headers)
        return response.headers['Last-Modified']
    else:
        return None


def load_weekly_cases(cases):
    """ Creates a weekly version of the daily cases dataframe, excluding the last (incomplete) week """

    print ("\nCreating weekly dataframe...\n")

    tempdf = cases.dailydf.loc[(cases.dailydf.index > '2020-02-29')]
    wdf = tempdf.groupby(['Area code','Area name','Area type'])['Cases'].resample('w').sum().reset_index()

    wdf['Week'] = wdf['Date'].dt.strftime('%Y%U').astype("int")
    wdf['Cases'] = wdf['Cases'].astype(int)

    # Drop last incomplete week 
    wdf = wdf.loc[(wdf['Week'] < wdf['Week'].max())]

    print(wdf.tail(5))

    cases.weeklydf = wdf  
    try:
        cases.redis_connection.set("CasesWeekly", cases.arrow_context.serialize(cases.weeklydf).to_buffer().to_pybytes())
    except redis.RedisError:
        print("Error updating redis CasesWeekly")
        return False

    return True


def load_summary_cases(cases):
    """ Returns a dataframe with summary stats for the given daily, weekly and population data frames """

    print ("\nCreating summary stats dataframe...\n")

    # Calculate totals for last 4 weeks, last 2 weeks and preceding 2 weeks
    
    wdflast4 = cases.weeklydf.loc[(cases.weeklydf['Week'] > cases.weeklydf['Week'].max() - 4)]
    wdflast2 = cases.weeklydf.loc[(cases.weeklydf['Week'] > cases.weeklydf['Week'].max() - 2)]
    wdfprev2 = cases.weeklydf.loc[( (cases.weeklydf['Week'] > cases.weeklydf['Week'].max() - 4) & (cases.weeklydf['Week'] <= cases.weeklydf['Week'].max() - 2))]

    # Summary stats for maps, tables

    aggdf = cases.dailydf.groupby(['Area code','Area name','Area type']).agg({'Cases' : ['sum','mean','max']})
    aggdf.columns = ['All Time Cases', 'Average Daily Cases', 'Peak Daily Cases']

    # Add in population data
    populationdf = get_population_df()
    summarydf = populationdf.join(aggdf)

    # Add in weekly summary totals
    summarydf = summarydf.join(wdflast4.groupby(['Area code','Area name','Area type']).agg(**{'Last 4 Weeks Cases' : ('Cases','sum')}))
    summarydf = summarydf.join(wdflast2.groupby(['Area code','Area name','Area type']).agg(**{'Cases in Last Fortnight' : ('Cases','sum')}))
    summarydf = summarydf.join(wdfprev2.groupby(['Area code','Area name','Area type']).agg(**{'Cases in Previous Fortnight' : ('Cases','sum')}))

    # get a daily dataset for last 30 days
    last30 = (datetime.datetime.strptime(cases.dailydf.index.max().strftime('%d/%m/%Y'), '%d/%m/%Y') - datetime.timedelta(days=31)).strftime(format='%Y-%m-%d')
    
    sdf = cases.dailydf.loc[cases.dailydf.index >= last30]
    sdf.reset_index(inplace=True)

    # pivot so we have a dataframe with dates as columns, then unpivot back into dataframe
    spivot = sdf.pivot_table(index=['Area code','Area name','Area type'],columns='Date',values='Cases',aggfunc=np.sum)
    unpivot = pd.DataFrame(spivot.to_records())
    unpivot.set_index(['Area code','Area name','Area type'],inplace=True)

    # Calculate slope & last 3, 7 day totals
    unpivot['Last 14 Days Trend Slope'] = unpivot.apply(calc_slope, axis=1)
    unpivot['Cases in Last 7 Days'] = unpivot.apply(calc_7, axis=1)

    # Join it with our main summary dataframe
    unpivot_join = unpivot[['Last 14 Days Trend Slope','Cases in Last 7 Days']]
    summarydf = summarydf.join(unpivot_join)
    #summarydf.info(verbose=True)

    # Calculate ratios
    summarydf['All Time Cases Per 1000 People'] = round(summarydf['All Time Cases']  / summarydf['Population'] * 1000,2)
    summarydf['Last 4 Weeks Cases Per 1000 People'] = round(summarydf['Last 4 Weeks Cases']  / summarydf['Population'] * 1000,2)
    summarydf['Fortnightly % Change'] = round((summarydf['Cases in Last Fortnight'] - summarydf['Cases in Previous Fortnight']) / summarydf['Cases in Previous Fortnight'] * 100,0)
    summarydf['Average Daily Cases'] = round(summarydf['Average Daily Cases'],1)
    summarydf['Last 7 Days Cases Per 1000 People'] = round(summarydf['Cases in Last 7 Days']  / summarydf['Population'] * 1000,2)
    #summarydf.info(verbose=True)

    # Fill NaN with 0, and force integers on cases columns (get converted to floats in merge)
    summarydf=summarydf.fillna(0)
    summarydf[['Population','All Time Cases','Peak Daily Cases','Last 4 Weeks Cases','Cases in Last Fortnight','Cases in Previous Fortnight']] = \
        summarydf[['Population','All Time Cases','Peak Daily Cases','Last 4 Weeks Cases','Cases in Last Fortnight','Cases in Previous Fortnight']].astype(int)

    # Reset index so we get normal area, name, type columns
    summarydf.reset_index(inplace=True)
    
    #summarydf.info(verbose=True)
    print("Summary dataframe loaded.")
    print(summarydf.head(5))
    cases.summarydf = summarydf

    try:
        cases.redis_connection.set("CasesSummary", cases.arrow_context.serialize(cases.summarydf).to_buffer().to_pybytes())
    except redis.RedisError:
        print("Error updating redis CasesSummary")
        return False

    return True


def calc_slope(row):
    """ Returns the slope of the preceding complete 14 days (we ignore last 2 days) of a dataframe row """

    filled = row.fillna(0)
    s = filled.rolling(window=7).mean().values
    return np.polyfit(list(range(14)),s[-16:-2],1)[0].round(2)


def calc_7(row):
    """ sum of last 7 days """
    s = row.fillna(0)
    return int(sum(s[-7:]))


def get_population_df():
    """ Returns a dataframe of population data by area code, and area name """

    popfile = os.path.dirname(__file__) + "/data/ukpopulation_rev.dat"
    popdf = pd.read_csv(popfile, index_col=[0,1,2],skiprows=0)
    popdf['Population'] = popdf['Population'].astype(int)
    print("Population dataframe loaded.")
    print(popdf.head(5))
    return popdf


def failure_check(month_totals):
    ''' Sanity checks monthly totals - returns True if checks fail '''

    if np.isnan(month_totals).any():
        print("NaN found in totals - failure detected")
        return True
    else:
        totalcases = sum(month_totals)
        print("Total:",totalcases)
        
        # Should have over 200k cases
        if totalcases < CASES_THRESHOLD:
            return True

    return False


def load_daily_cases(cases):
    ''' Main data load function - calls API for each hierarchy level we need and saves data to Redis
    to be picked up by the app '''

    print("\nLoading daily cases data... "+"\n")
    
    # Get list of months (yyyy-mm) to get data for    
    lastday = str(monthrange(int(strftime("%Y")),int(strftime("%m")))[1])
    month_list = pd.date_range("2020-02-01",strftime("%Y")+"-"+strftime("%m")+"-"+ lastday,freq='MS').strftime("%Y-%m").tolist()

    failed = False

    # Iterate through each hierarchy level (region, utla etc)
    for level in LEVELS_DICT:

        # Get data from API via for each month/level into dataframes and save to redis
        # Use Spark if we have >1 CPU - Overkill for this volume of data but it is still quicker 

        if multiprocessing.cpu_count() > 1:
            monthly_rdds = sc.parallelize(month_list)
            month_totals = monthly_rdds.map(lambda month : get_api_dataframe(level, month)).collect()
        else:
            month_totals = list(map(lambda month : get_api_dataframe(level, month) , month_list))

        print(level,month_totals)

        # Sanity check data
        failed = failure_check(month_totals)
        if failed:
            break

    if failed:
        print("Failed to pass sanity check. Stopping.")
        return False
    else:
        # Reload our saved redis data into daily dataframe
        df = EMPTY_DF
        df.set_index('Date',inplace=True)

        for key in cases.redis_connection.keys(pattern='Cases.*'):
            df = df.append(cases.arrow_context.deserialize(cases.redis_connection.get(key)))
        
        df = df.astype({'Cases': int, 'Tests': int, 'Hospital Cases': int, 'Deaths within 28 Days of Positive Test': int})

        cases.dailydf = df.sort_index()
        return True



def new_api_data_available(cases):
    ''' Checks API to see if new data is available, returns new timestamp if api data is newer '''

    latest_date_str = get_api_last_modified()

    if latest_date_str:

        print("API Last Modified:",latest_date_str)

        # Convert to datetime
        api_timestamp = datetime.datetime.strptime(latest_date_str,'%a, %d %b %Y %H:%M:%S %Z')

        # Get timestamp of our previously saved data
        data_timestamp = cases.get_cases_timestamp()

        print("API Data timestamp:",api_timestamp)
        print("App Data timestamp",data_timestamp)

        # If API data is newer then reload our data
        if api_timestamp > data_timestamp:  
            return api_timestamp

    else:
        print("No response from API.")

    return None


# Main ETL Pipeline
if __name__ == '__main__':

    print("\nStarting batch:",datetime.datetime.now())

    cases = CasesData(batch=True)

    if len(sys.argv) > 1:
        # Command line arg provided so override API date check
        print("Overriding API date check.")
        new_timestamp = datetime.datetime.now()
    else:    
        # Check if API has new data
        new_timestamp = new_api_data_available(cases)

    if new_timestamp:
        print("API Data is newer, reloading.....")

        # Load daily cases data        
        if load_daily_cases(cases):

            # Create weekly df
            if load_weekly_cases(cases):

                # Create summary stats df
                if load_summary_cases(cases):

                    # Update timestamp on data - this will trigger the dashboard to reload data
                    cases.redis_connection.set("data_timestamp", datetime.datetime.strftime(new_timestamp, '%Y-%m-%d %H:%M:%S') )

                    # Force redis disk write to flush previous changes
                    cases.redis_connection.bgrewriteaof()
                    
                    print("\nBatch Complete.")

    else:
        print("No new data to load.")

