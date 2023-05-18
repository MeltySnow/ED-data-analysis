import typing
import pandas as pd
import datetime
import time

class ExperimentMeta(object):
	"""
	Member variables:

	char *label
	datetime *startTime
	datetime *stopTime
	"""

	def __init__(self, notionDashboard: pd.DataFrame) -> None:
		self.label: str = notionDashboard.iloc[0].loc["Label"]
		startDatetimeString: datetime = notionDashboard.iloc[0].loc["Start Date & Time"].to_pydatetime()
		stopDatetimeString: datetime = notionDashboard.iloc[0].loc["End Date & Time"].to_pydatetime()

		# Convert times to UNIX epoch time
		self.startTime: float = self.ToUNIXTime(startDatetimeString)
		self.stopTime: float = self.ToUNIXTime(stopDatetimeString)

		#print (f"{self.startTime}, {self.stopTime}")
		#print (notionDashboard.iloc[0].loc["Start Date & Time"])
		#print (notionDashboard.iloc[0].loc["End Date & Time"])
	
	def ToUNIXTime(self, ip: datetime) -> float:
		return time.mktime(ip.timetuple())
