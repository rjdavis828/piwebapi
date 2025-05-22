#This is an example of how to use the PI Web API to pull data from a PI server

import pandas as pd
from dotenv import load_dotenv
import os
from pi_modules import PIWebAPI

load_dotenv(override=True)  # looks for .env in the current directory

#load environment variables
database = os.getenv("DATA_ARCHIVE")
base_url = os.getenv("BASE_URL")
auth = os.getenv("AUTH")
#connect to the PI Web API
# Example usage
conn  = PIWebAPI(base_url=base_url, auth=auth, verify=False)

#read taglist from a CSV file
taglist = pd.read_csv("taglist.csv")

if taglist.empty:
    print("Taglist is empty")
elif 'tags' not in taglist.columns:
    print("Taglist does not contain a column named 'tags'")
else:
    taglist = taglist["tags"].tolist()


starttime = "T-1d"
endtime = "T"


#pull summary data from the server
data_summary = conn.summary_data(dataserver=database, taglist=taglist, starttime=starttime, endtime=endtime, summaryDuration="1h", summaryType="Average")

#pull summary data from the server
data_compressed = conn.compressed_data(dataserver=database, taglist=taglist, starttime=starttime, endtime=endtime)

#pull interpolated data from the server
data_interpolated = conn.interpolated_data(dataserver=database, taglist=taglist, starttime=starttime, endtime=endtime, interval='5m')

