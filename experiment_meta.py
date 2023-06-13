import typing
import pandas as pd
import datetime
import time

class ExperimentMeta(object):
	"""
	Member variables:

	char *label;
	float startTime;
	float stopTime;
	dict processedData;
	"""

	def __init__(self, notionDashboard: pd.Series) -> None:
		self.label: str = notionDashboard.loc["Label"]
		startDatetimeString: datetime = notionDashboard.loc["Start Date & Time"].to_pydatetime()
		stopDatetimeString: datetime = notionDashboard.loc["End Date & Time"].to_pydatetime()

		# Convert times to UNIX epoch time (needed for InfluxDB query)
		self.startTime: float = self.ToUNIXTime(startDatetimeString)
		self.stopTime: float = self.ToUNIXTime(stopDatetimeString)

		#print (f"{self.label}: {self.startTime}, {self.stopTime}")

		#Forward declarations of member variables:
		self.processedData: dict = {
			"currentDensityActual" : [],
			"currentDensityCategorical" : [],
			"stackResistance" : [],
			"stackResistanceError" : [],
			"currentEfficiency" : [],
			"currentEfficiencyError" : [],
			"powerConsumption" : [],
			"powerConsumptionError" : [],
			"fluxCO2" : [],
			"fluxCO2Error" : [],
			"label" : [],
			"capturepHRange" : []
		}

	
	def ToUNIXTime(self, ip: datetime) -> float:
		return time.mktime(ip.timetuple())
