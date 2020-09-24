''' Batch export data to SQL database.   Not used in app - this is experimental '''

import sqlalchemy
from sqlalchemy import event
import redis
import pyarrow as pa
import pandas as pd
import urllib
import os
import time
import datetime

redis_connection = redis.Redis(host="localhost", port=6379, db=0)
arrow_context = pa.default_serialization_context()


def calc_deltas(level, month, newdf, redis, ac):
    ''' Work out deltas (not used in app - just experiment for future possible ETL to database) '''

    if redis.exists("Cases."+level+"."+month):
        previousdf = ac.deserialize(redis.get("Cases."+level+"."+month))
    else:
        previousdf = EMPTY_DF 

    previousdf.reset_index(inplace=True)
    newdf.reset_index(inplace=True)

    # Drop all zero rows
    previousdf = previousdf.loc[(previousdf['Cases']+previousdf['Tests']+previousdf['Hospital Cases']+previousdf['Deaths within 28 Days of Positive Test']) > 0]
    newdf = newdf.loc[(newdf['Cases']+newdf['Tests']+newdf['Hospital Cases']+newdf['Deaths within 28 Days of Positive Test']) > 0]

    diff_df = pd.merge(newdf,previousdf, how='outer', indicator='Exist')
    print(diff_df.head())
    
    print(previousdf.memory_usage().sum())
    print(diff_df.memory_usage().sum())

    deltas = diff_df.loc[diff_df['Exist'] == 'left_only']

    redis.set("Deltas."+level+"."+month, ac.serialize(deltas).to_buffer().to_pybytes())



def export_to_sqldb(key_pattern):
    ''' Exports redis data to sql database'''

    df = pd.DataFrame(columns=['Date','Area name','Area code','Area type','Cases','Tests','Hospital Cases','Deaths within 28 Days of Positive Test'])
    df.set_index('Date',inplace=True)

    if "SQLPARAMS" in os.environ:
        connection = sqlengine.connect()
        #redis_data = redis_connection.get(redis_key)

        print("Loading cases data from redis")
        for key in redis_connection.keys(pattern=key_pattern):
            print(key)
            df = df.append(arrow_context.deserialize(redis_connection.get(key)))

        print(df.head())
        print(df.tail())
        
        if df['Cases'].sum() > 0:
            
            print("Exporting data")

            print(df.shape[0], "rows")
            if df.shape[0] > 0:
                start = time.time()
                df.reset_index(inplace=True)
                df.to_sql(name="DeltaCases"+datetime.datetime.now().strftime("%Y%m%d"), con=connection, if_exists = 'replace', index=False, schema="Analysis")
                print(time.time() - start, "seconds")
            else:
                print("No rows to export")
        else:
            print("No delta cases found")
    else:
        print("SQL environment not defined")


# Set up SQL environment if sql environment variables are set

sqlengine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus(os.environ['SQLPARAMS'])))

@event.listens_for(sqlengine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    ''' Sets fast_executemany parameter to speed up sql export '''

    if executemany:
        cursor.fast_executemany = True



if __name__ == '__main__':

    export_to_sqldb("Deltas.*")
    #export_to_sqldb("CasesWeekly")
    #export_to_sqldb("CasesDaily")

