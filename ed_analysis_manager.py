from typing import Type, List
import requests, json
import numpy
import pandas as pd
import os
import time
import math
from datetime import datetime, timedelta
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import notion_df
from experiment_meta import ExperimentMeta
import sys
from dotenv import load_dotenv

#Meta-class with functionality that covers database queries, data processing and plotting graphs
class EDAnalysisManager(object):
	"""
	Member variables:

	pd.DataFrame notionDashboard;
	ExperimentMeta *Experiments;
	"""

	def __init__(self, argc: int, argv: List[str]) -> None:
		#Load env variables
		load_dotenv()
		self.NOTION_API_KEY = os.getenv("NOTION_API_KEY")
		self.NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
		self.INFLUXDB_API_KEY = os.getenv("INFLUXDB_API_KEY")
		self.INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")

		#Throw exception if env variables failed to load
		if not self.NOTION_API_KEY or not self.NOTION_DATABASE_ID or not self.INFLUXDB_API_KEY or not self.INFLUXDB_ORG:
			raise Exception("ERROR: secrets could not be loaded from .env file")

		self.FetchExperimentDataFromNotion()
		self.Experiments: List[ExperimentMeta]= []
		self.ParseExperimentMetadata(argc, argv)

		#for exp in self.Experiments:
			#rawData: pd.DataFrame = self.FetchFromInfluxDB(exp)
			#with open("debug.out", 'w', encoding="utf-8") as Writer:
			#	rawData.to_csv(Writer)


#Queries Notion and loads dashboard as pandas DataFrame
	def FetchExperimentDataFromNotion(self) -> None:
		#Setup constants
		tokenNotion = "secret_MaCxJctDQEzJCY6NNv5qdusCMnTXaNRTtHeh3HZlUm7"
		notionDatabaseID = "57136913df804361804a4da02b7c30f6"
		
		try:
			self.notionDashboard: pd.DataFrame = notion_df.download(notionDatabaseID, api_key=tokenNotion)
		except:
			print ("There was an error communicating with the Notion API", file=sys.stderr)
			sys.exit()





#Takes experiment ID and gets start and end timestamps from Notion database
	def ParseExperimentMetadata(self, argc: int, argv: List[str]) -> None:
		for n in range (1, argc):
			experimentID = argv[n]
			dashboardRow: pd.DashboardFrame = self.notionDashboard[self.notionDashboard["Experimental Name"] == experimentID]
			if dashboardRow.empty:
				print ("Warning: No experiment with ID \"%s\" was found" % (experimentID), file=sys.stderr)
			else:
				self.Experiments.append(ExperimentMeta(dashboardRow))




#Will take timestamps from Notion and use them to query InfluxDB, returning a pandas DataFrame with the raw experimental data
	def FetchFromInfluxDB(self, experimentMeta) -> pd.DataFrame:
		#Define constants for influxdb
		token: str = "D_m2xCnMofjaIn9OKSxIbirvJFrO1_LunpG9Z1mpR5wb-9vxTqIvgfjkueu5deRKaY6EepRdO6dTpl4YOROmBg=="
		org: str = "oran@missionzero.tech"
		url: str = "https://europe-west1-1.gcp.cloud2.influxdata.com"
		client: influxdb_client.InfluxDBClient = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
		
		#Debugging constants. Replace these with timestamps from Notion
		#START_TIME = "2023-05-09T15:07:00Z"
		#END_TIME = "2023-05-09T15:07:10Z"

		START_TIME = int(time.mktime(experimentMeta.startTime.timetuple()))
		END_TIME = int(time.mktime(experimentMeta.stopTime.timetuple()))

		query_api = client.query_api()

		influxQuery = f'\
	from(bucket: "MZT_Process_Components")\
	|> range(start: {START_TIME}, stop: {END_TIME})\
	|> filter(fn: (r) => r["_measurement"] == "component_value")\
	|> filter(fn: (r) => r["location"] == "arches")\
	|> filter(fn: (r) => r["stand_id"] == "ED002")\
	|> aggregateWindow(every: 10s, fn: mean, createEmpty: false)\
	|> pivot(rowKey:["_time"], columnKey: ["_field","component_id"], valueColumn: "_value")\
	|> yield(name: "ED Data")'
		print (influxQuery)

		return query_api.query_data_frame(org=org, query=influxQuery)


