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

#Class with functionality that covers database queries, data processing and plotting graphs
class EDAnalysisManager(object):
	"""
	Member variables:

	pd.DataFrame notionDashboard;
	ExperimentMeta *Experiments;
	"""

	def __init__(self, argc: int, argv: List[str]) -> None:
		#Load local env variables
		try:
			self.LoadEnvironmentVariables()
		except Exception as e:
			print (e, file=sys.stderr)
			sys.exit(1)

		#Request experiment metadata from Notion API
		self.FetchExperimentDataFromNotion()
		self.Experiments: List[ExperimentMeta]= []
		self.ParseExperimentMetadata(argc, argv)

		#Loop through Experiments list, request data from InfluxDB and process data
		self.ProcessData()

		#Plot processed data
		#self.PlotData()

	#Reads .env file in local directory and saves env variables as member variables
	def LoadEnvironmentVariables(self) -> None:
		load_dotenv()
		self.NOTION_API_KEY = os.getenv("NOTION_API_KEY")
		self.NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
		self.INFLUXDB_API_KEY = os.getenv("INFLUXDB_API_KEY")
		self.INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")

		#Throw exception if env variables failed to load
		if not (self.NOTION_API_KEY and self.NOTION_DATABASE_ID and self.INFLUXDB_API_KEY and self.INFLUXDB_ORG):
			raise Exception("ERROR: secrets could not be loaded from .env file")


#Queries Notion and loads dashboard as pandas DataFrame
	def FetchExperimentDataFromNotion(self) -> None:
		try:
			self.notionDashboard: pd.DataFrame = notion_df.download(self.NOTION_DATABASE_ID, api_key=self.NOTION_API_KEY)
		except:
			print ("There was an error communicating with the Notion API", file=sys.stderr)
			sys.exit(1)




#Takes experiment IDs and gets start and end timestamps from Notion database
	def ParseExperimentMetadata(self, argc: int, argv: List[str]) -> None:
		#Iterate through command line arguments, match them with experiment IDs in the notion database, use the DataFrame row to initialise and ExperimentMeta object and append to self.Experiments
		if argc > 1:
			for n in range (1, argc):
				experimentID = argv[n]
				dashboardRow: pd.DataFrame = self.notionDashboard[self.notionDashboard["Experimental Name"] == experimentID]
				if dashboardRow.empty:
					print ("Warning: No experiment with ID \"%s\" was found" % (experimentID), file=sys.stderr)
				else:
					self.Experiments.append(ExperimentMeta(dashboardRow.iloc[0]))

		#If no command arguments are passed, default to adding all experiments with the "Completed" field ticked
		else:
			#Filter irrelevant columns out of the dashboard
			relevantDashboard: pd.DataFrame = self.notionDashboard[self.notionDashboard["Completed"]]
			if relevantDashboard.empty:
				print ("Error: No experiment IDs were passed, and no completed experiments were found in the Notion dashboard", file=sys.stderr)
				sys.exit(1)
			for index, row in relevantDashboard.iterrows():
				self.Experiments.append(ExperimentMeta(row))




	#Dependency for ProcessData(). Takes timestamps from ExperimentMeta object, queries InfluxDB and returns a pandas DataFrame with the raw experimental data
	def FetchFromInfluxDB(self, experimentMeta: ExperimentMeta) -> pd.DataFrame:
		#Setup InfluxDB client
		url: str = "https://europe-west1-1.gcp.cloud2.influxdata.com"
		client: influxdb_client.InfluxDBClient = influxdb_client.InfluxDBClient(url=url, token=self.INFLUXDB_API_KEY, org=self.INFLUXDB_ORG)
		
		#Query only allows integral timestamps
		START_TIME: int = int(experimentMeta.startTime)
		STOP_TIME: int = int(experimentMeta.stopTime)

		query_api = client.query_api()

		influxQuery: str = f'\
	from(bucket: "MZT_Process_Components")\
	|> range(start: {START_TIME}, stop: {STOP_TIME})\
	|> filter(fn: (r) => r["_measurement"] == "component_value")\
	|> filter(fn: (r) => r["location"] == "arches")\
	|> filter(fn: (r) => r["stand_id"] == "ED002")\
	|> aggregateWindow(every: 10s, fn: mean, createEmpty: false)\
	|> pivot(rowKey:["_time"], columnKey: ["_field","component_id"], valueColumn: "_value")\
	|> yield(name: "ED Data")'


		return query_api.query_data_frame(org=self.INFLUXDB_ORG, query=influxQuery)


	def ProcessData(self) -> None:
		for exp in self.Experiments:
			#Create DataFrame with data for a single experiment
			rawData: pd.DataFrame = self.FetchFromInfluxDB(exp)

			#Logic to guess the timestamps for the last 5 minutes of each current density setting. Based on deviation of current reading from a rolling median
			#Lots of magic numbers here, sorry :(
			#They gave good results for me, but feel free to play around with them if they aren't working out for you
			roll: int = 5
			percentTolerance: float = 10.0
			n: int = roll * 2 #index for while loop
			currents: pd.Series = rawData["current_PSU001"]
			sliceIndices: List(int) = []
			while n < currents.size:
				rollingMedian: float =  currents.rolling(roll).median()[n]
				upperBound: float = rollingMedian * (1 + (percentTolerance/100.0))
				lowerBound: float = rollingMedian * (1 - (percentTolerance/100.0))
				if currents[n] > upperBound or currents[n] < lowerBound:
					sliceIndices.append(n - 1) #n should NOT be included
					n += roll * 2 #More magic numbers, sorry
				n += 1

			sliceIndices.append(currents.size - 5) #need an index for the endpoint as well

			#Now we're gonna loop through the indices and calculate the key metrics for each current density
			for ind in sliceIndices:
				endTimestamp: datetime = rawData["_time"][ind]
				startTimestamp: datetime = endTimestamp - timedelta(minutes=5)
				dataWindow: pd.DataFrame = rawData[rawData["_time"] >= startTimestamp]
				dataWindow = dataWindow[dataWindow["_time"] <= endTimestamp]

				#Now we used the sliced data to work out the key metrics, and add them to the processedData dictionary in the ExperimentMeta classes

				#Get current density (actual, and a categorically grouped version for graph plotting)
				currentDensityTuple: Tuple[float, int] = ed_metric_calculations.GetCurrentDensity(dataWindow)

				#Ensure calculation was successful
				if math.isnan(currentDensityTuple[0]) or math.isnan(currentDensityTuple[1]):
					print ("Warning: error in calculating current density for experiment labelled \"%s\"" % exp.label, file=sys.stderr)
					currentDensityTuple = (0.0, 0.0)

				exp.processedData["currentDensityActual"].append(currentDensityTuple[0])
				exp.processedData["currentDensityCategorical"].append(currentDensityTuple[1])

				#Get stack resistance
				stackResistanceTuple: Tuple[float, float] = ed_metric_calculations.GetStackResistance(dataWindow)

				#Ensure calculation was successful
				if math.isnan(stackResistanceTuple[0]) or math.isnan(stackResistanceTuple[1]):
					print ("Warning: error in calculating stack resistance for experiment labelled:\n\t\"%s\"\n\tat current density: %f A/m^2" % (exp.label, currentDensityTuple[0]), file=sys.stderr)
					stackResistanceTuple = (0.0, 0.0)

				exp.processedData["stackResistance"].append(stackResistanceTuple[0])
				exp.processedData["stackResistanceError"].append(stackResistanceTuple[1])

				#Get current efficiency
				currentEfficiencyTuple: Tuple[float, float] = ed_metric_calculations.GetCurrentEfficiency(dataWindow)

				#Ensure calculation was successful
				if math.isnan(currentEfficiencyTuple[0]) or math.isnan(currentEfficiencyTuple[1]):
					print ("Warning: error in calculating current efficiency for experiment labelled:\n\t\"%s\"\n\tat current density: %f A/m^2" % (exp.label, currentDensityTuple[0]), file=sys.stderr)
					currentEfficiencyTuple = (0.0, 0.0)

				exp.processedData["currentEfficiency"].append(currentEfficiencyTuple[0])
				exp.processedData["currentEfficiencyError"].append(currentEfficiencyTuple[1])

				#Get power consumption
				powerConsumptionTuple: Tuple[float, float] = ed_metric_calculations.GetPowerConsumption(dataWindow)

				#Ensure calculation was successful
				if math.isnan(powerConsumptionTuple[0]) or math.isnan(powerConsumptionTuple[1]):
					print ("Warning: error in calculating power consumption for experiment labelled:\n\t\"%s\"\n\tat current density: %f A/m^2" % (exp.label, currentDensityTuple[0]), file=sys.stderr)
					powerConsumptionTuple = (0.0, 0.0)

				exp.processedData["powerConsumption"].append(powerConsumptionTuple[0])
				exp.processedData["powerConsumptionError"].append(powerConsumptionTuple[1])

				#Get CO2 flux
				fluxCO2Tuple: Tuple[float, float] = ed_metric_calculations.GetCO2Flux(dataWindow)

				#Ensure calculation was successful
				if math.isnan(fluxCO2Tuple[0]) or math.isnan(fluxCO2Tuple[1]):
					print ("Warning: error in calculating CO2 flux for experiment labelled:\n\t\"%s\"\n\tat current density: %f A/m^2" % (exp.label, currentDensityTuple[0]), file=sys.stderr)
					fluxCO2Tuple = (0.0, 0.0)

				exp.processedData["fluxCO2"].append(fluxCO2Tuple[0])
				exp.processedData["fluxCO2Error"].append(fluxCO2Tuple[1])
				
				#I don't like doing this, but plotly needs it
				exp.processedData["label"].append(exp.label)


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
			Writer.write("""\
<!DOCTYPE html>
<html>
<head>
	<title>ED results</title>
	<style>
		.graph-column{
			width: 45%;
			float: left;
			padding: 5px 12px 0px 0px;
		}
	</style>
</head>
<body>
	<div class=\"graph-row\">\n"""
		)
			
			for n in range(0, len(plots)):
				Writer.write("<div class=\"graph-column\">\n")
				Writer.write(plots[n].to_html(full_html=False))
				Writer.write("</div>")
				if n % 2 == 1:
					Writer.write("\t</div>\n")
					if n < (len(plots) - 1):
						Writer.write("\t<div class=\"graph-row\">\n")

			Writer.write("</body>\n</html>")
