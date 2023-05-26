#Import pip packages
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
import sys
from dotenv import load_dotenv
import plotly.express as px
#Import project files
from experiment_meta import ExperimentMeta
import ed_metric_calculations

#Meta-class with functionality that covers database queries, data processing and plotting graphs
class EDAnalysisManager(object):
	"""
	Member variables:

	pd.DataFrame notionDashboard;
	ExperimentMeta *Experiments;
	"""

	def __init__(self, argc: int, argv: List[str]) -> None:
		#Load env variables
		#print ("Loading environment variables...")
		self.LoadEnvironmentVariables()

		#Request experiment metadata from Notion API
		#print ("Requesting metadata from Notion...")
		self.FetchExperimentDataFromNotion()
		#print ("Parsing experiment metadata...")
		self.Experiments: List[ExperimentMeta]= []
		self.ParseExperimentMetadata(argc, argv)

		#Loop through Experiments list, request data from InfluxDB and process data
		self.ProcessData()

		#Plot processed data
		self.PlotData()


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
			#print ("Requesting data from InfluxDB for experiment: %s..." % (exp.label))
			rawData: pd.DataFrame = self.FetchFromInfluxDB(exp)

			#Turn timestamps into seconds since start of experiment
			#Find the timestamps at which to slice the dataframe (point where the current changes)
			#Average these ranges & do arithmetic operations on them

			#Loop through currents to get row indices at which the current changes
			#print ("Processing data...\n")
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

				#Now we used the sliced data to work out the key metrics and add them to the dictionaries in the experimentMeta classes
				currentDensityTuple: Tuple[float, int] = ed_metric_calculations.GetCurrentDensity(dataWindow)
				exp.processedData["currentDensityActual"].append(currentDensityTuple[0])
				exp.processedData["currentDensityCategorical"].append(currentDensityTuple[1])
				try:
					stackResistanceTuple: Tuple[float, float] = ed_metric_calculations.GetStackResistance(dataWindow)
					exp.processedData["stackResistance"].append(stackResistanceTuple[0])
					exp.processedData["stackResistanceError"].append(stackResistanceTuple[1])
				except Exception as e:
					print (e, file=sys.stderr)
				try:
					currentEfficiencyTuple: Tuple[float, float] = ed_metric_calculations.GetCurrentEfficiency(dataWindow)
					exp.processedData["currentEfficiency"].append(currentEfficiencyTuple[0])
					exp.processedData["currentEfficiencyError"].append(currentEfficiencyTuple[1])
				except Exception as e:
					print (e, file=sys.stderr)

				try:
					powerConsumptionTuple: Tuple[float, float] = ed_metric_calculations.GetPowerConsumption(dataWindow)
					exp.processedData["powerConsumption"].append(powerConsumptionTuple[0])
					exp.processedData["powerConsumptionError"].append(powerConsumptionTuple[1])
				except Exception as e:
					print (e, file=sys.stderr)

				try:
					fluxCO2Tuple: Tuple[float, float] = ed_metric_calculations.GetCO2Flux(dataWindow)
					exp.processedData["fluxCO2"].append(fluxCO2Tuple[0])
					exp.processedData["fluxCO2Error"].append(fluxCO2Tuple[1])
				except Exception as e:
					print (e, file=sys.stderr)
				
				#I don't like doing this, but plotly needs it
				exp.processedData["label"].append(exp.label)


			#with open("debug.out", 'w', encoding="utf-8") as Writer:
			#	rawData.to_csv(Writer)


	def PlotData(self) -> None:
		#Combine all processed data into 1 dataframe:
		allProcessedData: pd.DataFrame = pd.DataFrame()
		for exp in self.Experiments:
			allProcessedData = pd.concat([allProcessedData, pd.DataFrame(exp.processedData)], ignore_index=True)


		"""
		with open("debug.out", 'w', encoding="utf-8") as Writer:
			allProcessedData.to_csv(Writer)
		"""
		#Make list of plots:
		plots: List[px.plot] = []

		#Actual plotting code:
		plots.append(px.bar(allProcessedData,
			x="currentDensityCategorical",
			y="stackResistance",
			error_y="stackResistanceError",
			color="label",
			barmode="group"
		))

		plots[0].update_layout(
			title="Stack resistance",
			legend_title="Amine",
			xaxis_title="Current density / A m<sup>-2</sup>",
			yaxis_title="Stack resistance / Ω"
		)


		plots.append(px.bar(allProcessedData,
			x="currentDensityCategorical",
			y="currentEfficiency",
			error_y="currentEfficiencyError",
			color="label",
			barmode="group"
			))

		plots[1].update_layout(
			title="Current efficiency",
			legend_title="Amine",
			xaxis_title="Current density / A m<sup>-2</sup>",
			yaxis_title="Current efficiency / %"
		)

		plots.append(px.bar(allProcessedData,
			x="currentDensityCategorical",
			y="powerConsumption",
			error_y="powerConsumptionError",
			color="label",
			barmode="group"
			))

		plots[2].update_layout(
			title="Power consumption",
			legend_title="Amine",
			xaxis_title="Current density / A m<sup>-2</sup>",
			yaxis_title="Power consumption / kWh t<sup>-1</sup> CO<sub>2</sub>"
		)

		plots.append(px.bar(allProcessedData,
			x="currentDensityCategorical",
			y="fluxCO2",
			error_y="fluxCO2Error",
			color="label",
			barmode="group",
			))

		plots[3].update_layout(
			title="CO<sub>2</sub> flux",
			legend_title="Amine",
			xaxis_title="Current density / A m<sup>-2</sup>",
			yaxis_title="CO<sub>2</sub> flux / mg m<sup>-2</sup> s<sup>-1</sup>"
		)

		#Add plots to HTML doc:
		with open("out.html", 'w', encoding="utf-8") as Writer:
			Writer.write("<!DOCTYPE html>\n<html>\n<head>\n\t<title>ED results</title>\n\t<style>\n\t\t.graph-column{\n\t\t\twidth: 45%;\n\t\t\tfloat: left;\n\t\t\tpadding: 5px 12px 0px 0px;\n\t\t}\n\t</style>\n</head>\n<body>\n\t<div class=\"graph-row\">\n")
			for n in range(0, len(plots)):
				Writer.write("<div class=\"graph-column\">\n")
				Writer.write(plots[n].to_html(full_html=False))
				Writer.write("</div>")
				if n % 2 == 1:
					Writer.write("\t</div>\n")
					if n < (len(plots) - 1):
						Writer.write("\t<div class=\"graph-row\">\n")

			Writer.write("</body>\n></html>")
