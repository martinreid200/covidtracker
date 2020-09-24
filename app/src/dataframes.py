import pandas as pd
import datetime
import time
import json
import os
import pyarrow as pa
import redis


class CasesData:
    ''' Main Cases class that holds all our case and reference data frames and hierachies 
        batch parameter is set if we are running our batch etl process outside docker container - hence different redis host '''

    def __init__(self, batch=None):
        
        # Set up redis host.  Use localhost if running from batch, otherwise default to "cache" (docker container)
        if batch:
            redis_host = "localhost"
        else:
            redis_host = "cache"

        print("Redis Host:",redis_host)

        self.redis_connection = redis.Redis(host=redis_host, port=6379, db=0)
        self.arrow_context = pa.default_serialization_context()

        # Static lists
        self.levels = ['Nation','Region','Upper tier local authority','Lower tier local authority']

        self.map_measures = ["Last 4 Weeks Cases Per 1000 People", "Fortnightly % Change", 
                                "All Time Cases Per 1000 People", "Last 14 Days Trend Slope",
                                "Cases in Last 7 Days", "Last 7 Days Cases Per 1000 People"
                            ]

        self.measures_availability = {
            'Cases': self.levels,
            'Average': self.levels,
            'Tests': [self.levels[0]],
            'Hospital Cases': [self.levels[0]],
            'Average Hospital Cases': [self.levels[0]],
            'Deaths within 28 Days of Positive Test': [self.levels[0]],
            'Average Deaths': [self.levels[0]]
        }

        # Static Geo mapping data

        self.geo_data = {
            self.levels[1] : { "key" : "rgn19cd" , "data" : self.get_geodata("ukregionsgeo.json") },
            self.levels[2] : { "key" : "ctyua17cd" , "data" : self.get_geodata("ukcountygeo.json") },
            self.levels[3] : { "key" : "lad19cd" , "data" : self.get_geodata("uklocalauthgeo.json") }
        }

        # Empty dataframes & variables.

        self.dailydf=pd.DataFrame(columns=['Date','Area name','Area code','Area type','Cases','Tests','Hospital Cases','Deaths within 28 Days of Positive Test'])
        self.dailydf.set_index('Date',inplace=True)
        self.weeklydf=pd.DataFrame(columns=['Area code','Area name','Area type','Date','Cases','Tests','Hospital Cases','Deaths within 28 Days of Positive Test','Week'])
        self.summarydf=pd.DataFrame(columns=['Area code','Area name','Area type','Population','Last 4 Weeks Cases Per 1000 People','Fortnightly % Change','Last 7 Days Cases Per 1000 People','Last 3 Days Cases Per 1000 People'])

        self.latest_case_date = ""
        self.latest_complete_week = ""
        self.latest_data_load_timestamp = None
        self.arealist = []
        self.hierachy = {}

        # Load saved dataframes
        self.load()


    def load(self):
        ''' Method to load cases data from Redis into daily, weekly and summary dataframes. 
            Called from app serve layout function '''

        # Check if we have new data by checking timestamp of saved cases in redis
        data_timestamp = self.get_cases_timestamp()

        print("\nWeb App Data last refreshed",self.latest_data_load_timestamp)
        print("Redis data timestamp",data_timestamp)

        if self.latest_data_load_timestamp != data_timestamp:
            
            print("Redis data updated, Loading new data.....")

            # Make sure we have redis data
            if self.redis_connection.exists("CasesSummary"):

                # Set timestamp so we don't load it again until the next batch refresh
                self.latest_data_load_timestamp = data_timestamp

                # Load Daily data from redis keys

                print("Loading cases data from redis")
                self.dailydf=pd.DataFrame(columns=['Date','Area name','Area code','Area type','Cases','Tests','Hospital Cases','Deaths within 28 Days of Positive Test'])
                for key in self.redis_connection.keys(pattern='Cases.*'):
                    self.dailydf = self.dailydf.append(self.arrow_context.deserialize(self.redis_connection.get(key)))
                self.dailydf = self.dailydf.astype({'Cases': int, 'Tests': int, 'Hospital Cases': int, 'Deaths within 28 Days of Positive Test': int})
                self.dailydf.sort_index(inplace=True)
                
                self.latest_case_date = self.dailydf.index.max().strftime('%d/%m/%Y')
                self.date_index = pd.date_range('2020-02-29', self.dailydf.index.max())

                # Area lists & hierachy
                self.arealist = sorted(self.dailydf['Area name'].unique())

                for level in self.levels:
                    self.hierachy.update({level : sorted(self.dailydf.loc[self.dailydf['Area type'] == level]['Area name'].unique())})

                # Weekly data
                self.weeklydf = self.arrow_context.deserialize(self.redis_connection.get("CasesWeekly"))

                day = int(self.weeklydf['Date'].max().strftime("%d"))
                if 4 <= day <= 20 or 24 <= day <= 30:
                    suffix = "th"
                else:
                    suffix = ["st", "nd", "rd"][day % 10 - 1]

                self.latest_complete_week = str(day) + suffix + " " +self.weeklydf['Date'].max().strftime("%b")

                # Summary stats
                self.summarydf = self.arrow_context.deserialize(self.redis_connection.get("CasesSummary"))

                print ("LATEST CASES",self.latest_case_date)
                print ("LATEST COMPLETE WEEK",self.latest_complete_week)

            else:
                print("CasesDaily not found in Redis cache.  No data!!!!.")


    def get_geodata(self, filename):
        ''' Returns json geo data from given file '''

        with open(os.path.dirname(__file__) + "/data/" + filename,encoding="utf-8") as f:
            return json.load(f)


    def get_cases_timestamp(self):
        """ Returns timestamp of latest saved cases data (or old date if we don't have one yet) """
        latest=None
        try:
            latest = self.redis_connection.get("data_timestamp")
        except redis.ConnectionError:
            print("Unable to get redis cases timestamp.  Using default.")

        if latest:
            return datetime.datetime.strptime(latest.decode(),'%Y-%m-%d %H:%M:%S')
        else:
            return datetime.datetime(1970, 1, 1, 12, 00, 00)


    def get_plot_level(self, areaname):
        ''' Method to find a hierachy level for a given area (lower tier authorities have priority) '''

        if areaname in self.hierachy[self.levels[3]]:
            return self.levels[3]
        elif areaname in self.hierachy[self.levels[2]]:
            return self.levels[2]
        elif areaname in self.hierachy[self.levels[1]]:
            return self.levels[1]
        else:
            return self.levels[0]
