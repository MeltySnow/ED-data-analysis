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
		print ("Loading environment variables...")
		self.LoadEnvironmentVariables()

		#Request experiment metadata from Notion API
		print ("Requesting metadata from Notion...")
		self.FetchExperimentDataFromNotion()
		print ("Parsing experiment metadata...")
		self.Experiments: List[ExperimentMeta]= []
		self.ParseExperimentMetadata(argc, argv)

		#Loop through Experiments list, request data from InfluxDB and process data
		self.ProcessData()


	def LoadEnvironmentVariables(self) -> None:
		load_dotenv()
		self.NOTION_API_KEY = os.getenv("NOTION_API_KEY")
		self.NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
		self.INFLUXDB_API_KEY = os.getenv("INFLUXDB_API_KEY")
		self.INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")

		#Throw exception if env variables failed to load
		if not self.NOTION_API_KEY or not self.NOTION_DATABASE_ID or not self.INFLUXDB_API_KEY or not self.INFLUXDB_ORG:
			raise Exception("ERROR: secrets could not be loaded from .env file")


#Queries Notion and loads dashboard as pandas DataFrame
	def FetchExperimentDataFromNotion(self) -> None:
		try:
			self.notionDashboard: pd.DataFrame = notion_df.download(self.NOTION_DATABASE_ID, api_key=self.NOTION_API_KEY)
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
		url: str = "https://europe-west1-1.gcp.cloud2.influxdata.com"
		client: influxdb_client.InfluxDBClient = influxdb_client.InfluxDBClient(url=url, token=self.INFLUXDB_API_KEY, org=self.INFLUXDB_ORG)
		
		#Query only allows integer timestamps
		START_TIME: int = int(experimentMeta.startTime)
		END_TIME: int = int(experimentMeta.stopTime)

		query_api = client.query_api()

		influxQuery: str = f'\
	from(bucket: "MZT_Process_Components")\
	|> range(start: {START_TIME}, stop: {END_TIME})\
	|> filter(fn: (r) => r["_measurement"] == "component_value")\
	|> filter(fn: (r) => r["location"] == "arches")\
	|> filter(fn: (r) => r["stand_id"] == "ED002")\
	|> aggregateWindow(every: 10s, fn: mean, createEmpty: false)\
	|> pivot(rowKey:["_time"], columnKey: ["_field","component_id"], valueColumn: "_value")\
	|> yield(name: "ED Data")'
		#print (influxQuery)

		return query_api.query_data_frame(org=self.INFLUXDB_ORG, query=influxQuery)


	def ProcessData(self) -> None:
		for exp in self.Experiments:
			print ("Requesting data from InfluxDB for experiment: %s..." % (exp.label))
			rawData: pd.DataFrame = self.FetchFromInfluxDB(exp)
			#processedData: pd.DataFrame = pd.DataFrame()

			#Turn timestamps into seconds since start of experiment
			#Find the timestamps at which to slice the dataframe (point where the current changes)
			#Average these ranges & do arithmetic operations on them

			#Loop through currents to get row indices at which the current changes
			print ("Processing data...\n")
			roll: int = 5
			currents: pd.Series = rawData["current_PSU001"]
			percentTolerance: float = 10.0
			n: int = roll * 2
			sliceIndices: List(int) = []
			while n < currents.size:
				rollingMedian: float =  currents.rolling(roll).median()[n]
				upper: float = rollingMedian * (1 + (percentTolerance/100.0))
				lower: float = rollingMedian * (1 - (percentTolerance/100.0))
				if currents[n] > upper or currents[n] < lower:
					#print ("%i, %f" % (n, currents[n]))
					sliceIndices.append(n - 1)#Because n should NOT be included
					n += 5
				n += 1
			sliceIndices.append(currents.size - 5)#need one for the endpoint as well lol

			#Now we're gonna loop through the indices to get values for each current density
			for ind in sliceIndices:
				endTimestamp: datetime = rawData["_time"][ind]
				startTimestamp: datetime = endTimestamp - timedelta(minutes=5)
				dataWindow: pd.DataFrame = rawData[rawData["_time"] >= startTimestamp]
				dataWindow = dataWindow[dataWindow["_time"] <= endTimestamp]
				#print (dataWindow["current_PSU001"])

				#Write these functions later, and save their outputs to some sort of appropriate data structure. I'm thinking some sorta 2D array, either as a member of this class (EDAnalysisManager) or as a member of the ExperimentMeta class
				GetStackResistance(dataWindow)
				GetCurrentEfficiency(dataWindow)
				GetPowerConsumption(dataWindow)
				GetCO2Flux(dataWindow)


			#with open("debug.out", 'w', encoding="utf-8") as Writer:
			#	rawData.to_csv(Writer)
