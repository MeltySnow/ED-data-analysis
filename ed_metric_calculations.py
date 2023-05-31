from typing import Type, Tuple
import pandas as pd
import sys
import math
import datetime
import time

#Class that is initialised using a slice of a DataFrame and calculates key performance metrics
class EDMetrics(object):
	def __init__(self, inputDataWindow: pd.DataFrame) -> None:
		#Define constants used in calculations
		self.MEMBRANE_AREA: float = 0.0036 #m^2
		self.FARADAY_CONSTANT: float = 96485.0 #C mol^{-1}
		self.CO2_DENSITY: float = 1.815 #g dm^{-3}
		self.MEMBRANE_PAIRS: float = 10.0 #dimensionless
		self.CO2_MOLAR_MASS: float = 44.01 #g mol^{-1}
		self.BICARBONATE_CHARGE: float = 1.0 #dimensionless

		#Make input DataFrame available to all member functions
		self.dataWindow: pd.DataFrame = inputDataWindow

		#Load some derived values that are often reused across key metric calculations
		self.totalMolesCO2: Tuple[float, float] = self.GetMolesCO2()

#########################################################
#DEFINE STATIC FUNCTIONS WITH BASIC ARITHMETIC OPERATIONS
#########################################################
	#Division, with functionality for combining errors
	@staticmethod
	def ErrorDivide(a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
		outputValue = a[0] / b[0]
		relativeError = (a[1]/a[0]) + (b[1]/b[0]) 
		outputError = outputValue * relativeError 
		return (outputValue, outputError)

	#Multiplication, with functionality for combining errors
	@staticmethod
	def ErrorMultiply(a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
		outputValue = a[0] * b[0]
		relativeError = (a[1]/a[0]) + (b[1]/b[0]) 
		outputError = outputValue * relativeError 
		return (outputValue, outputError)

	#Converts a datetime-type string to a float representing its UNIX timestamp in seconds
	@staticmethod
	def ToUNIXTime(dateString: datetime) -> float:
		return time.mktime(dateString.timetuple())

	#Integrates series y wrt series x by drawing trapezia between each set of data points and adding their areas
	@staticmethod
	def Integrate(xSeries: pd.Series, ySeries: pd.Series) -> float:
		#Initialize variables for integration loop
		integral: float = 0.0
		x: float = 0.0
		y: float = 0.0
		x1: float = xSeries.iloc[0]
		y1: float = ySeries.iloc[0]
		seriesLength: int = xSeries.size

		#Ensure the length of the two series matches. If not, sets seriesLength to that of the smaller series so as to avoid an index out of range error in the loop
		if seriesLength != ySeries.size:
			print ("WARNING: integration error: lengths of x and y series do not match", file=sys.stderr)
			if seriesLength > ySeries.size:
				seriesLength = ySeries
		
		#Actual integration loop
		for n in range(1, seriesLength):
			x = x1
			y = y1
			x1 = xSeries.iloc[n]
			y1 = ySeries.iloc[n]

			trapeziumArea: float = ((y + y1) / 2.0) * (x1 - x)
			integral += trapeziumArea
		
		return integral

############################################
#DEFINE PRIVATE, NON-STATIC MEMBER FUNCTIONS
############################################

	def GetMolesCO2(self) -> Tuple[float, float]:
		#Convert CO2 ppm into fraction of CO2
		co2FractionSeries: pd.Series = self.dataWindow["CO2_PPM_CO2001"].apply(lambda x: x / 1000000.0)
		#Convert air volumetric flow from litres/minute to litres/second
		airVolumetricFlowSeries: pd.Series = self.dataWindow["volumetric_flow_MFM001"].apply(lambda x: x / 60.0)
		#Convert timestamp from some ugly string to UNIX timestamp
		timeSeries: pd.Series = self.dataWindow["_time"].apply(self.ToUNIXTime)

		#Combine CO2 fraction and air volumetric flow series to get CO2 volume
		co2VolumeSeries: pd.Series = co2FractionSeries.multiply(airVolumetricFlowSeries, fill_value=0.0)

		#Get error of CO2 volume
		co2VolumeError: float = co2VolumeSeries.std()
		#Get total CO2 volume via integration over time
		totalCO2Volume: float = self.Integrate(timeSeries, co2VolumeSeries)

		#Combine value and error into tuple
		outputTuple: Tuple[float, float] = (totalCO2Volume, co2VolumeError)

		#Convert L CO2 to g CO2
		outputTuple = self.ErrorMultiply(outputTuple, (self.CO2_DENSITY, 0.0))
		#Convert g CO2 to mol CO2
		outputTuple = self.ErrorDivide(outputTuple, (self.CO2_MOLAR_MASS, 0.0))
		
		return outputTuple

###########################################
#DEFINE PUBLIC, NON-STATIC MEMBER FUNCTIONS
###########################################

	#Returns a tuple. 0th element is actual current density, 1st is categorical current density needed for bar plotting
	def GetCurrentDensity(self) -> Tuple[float, int]:
		#Parses current densities into well-defined categories to group bars together
		currentDensities: List[float] = [120.0, 200.0, 280.0, 360.0, 440.0, 520.0]

		#Calculate ACTUAL current density
		#Extract current from dataframe:
		actualCurrentDensity: float = self.dataWindow["current_PSU001"].mean() / self.MEMBRANE_AREA

		#Now we see which category the actual value is closest to
		outputIndex: int = 0
		for n in range(1, len(currentDensities)):
			diff: float = abs(currentDensities[n] - actualCurrentDensity)
			best: float = abs(currentDensities[outputIndex] - actualCurrentDensity)
			if diff < best:
				outputIndex = n
		return (actualCurrentDensity, int(currentDensities[outputIndex]))


#In the following functions, numbers are stored as tuples of format (data, error)

	def GetStackResistance(self) -> Tuple[float, float]:
		#Extract values and errors from dataframe:
		current: Tuple[float, float] = (self.dataWindow["current_PSU001"].mean(), self.dataWindow["current_PSU001"].std())
		voltage: Tuple[float, float] = (self.dataWindow["voltage_PSU001"].mean(), self.dataWindow["voltage_PSU001"].std())

		#Perform arithmetic
		resistance = self.ErrorDivide(voltage, current)
		return resistance

	def GetCurrentEfficiency(self) -> Tuple[float, float]:
		#Extract values and errors from dataframe:
		currentSeries: pd.Series = self.dataWindow["current_PSU001"]
		timeSeries: pd.Series = self.dataWindow["_time"].apply(self.ToUNIXTime)

		#Begin arithmetic
		currentEfficiency: Tuple[float, float] = (0.0, 0.0)

		#Work out total number of mol of electrons passed:
		molElectrons: Tuple[float, float] = (self.Integrate(timeSeries, currentSeries), currentSeries.std()) #Gives total coulombs passed
		molElectrons = self.ErrorDivide(molElectrons, (self.FARADAY_CONSTANT, 0.0))

		#Work out mol of CO2 per mol of e-
		currentEfficiency = self.ErrorDivide(self.totalMolesCO2, molElectrons)
		#Convert to %
		currentEfficiency = self.ErrorMultiply(currentEfficiency, (100.0, 0.0))
		#Work out CE per cell pair
		currentEfficiency = self.ErrorDivide(currentEfficiency, (self.MEMBRANE_PAIRS, 0.0))

		return currentEfficiency	


	def GetPowerConsumption(self) -> Tuple[float, float]:
		#Extract values and errors from dataframe:
		powerSeries: pd.Series = self.dataWindow["current_PSU001"].multiply(self.dataWindow["voltage_PSU001"])
		timeSeries: pd.Series = self.dataWindow["_time"].apply(self.ToUNIXTime)

		#Begin arithmetic
		powerConsumption: Tuple[float, float] = (0.0, 0.0)

		#Work out total energy in J
		totalEnergy: Tuple[float, float] = (self.Integrate(timeSeries, powerSeries), powerSeries.std())

		#Convert energy to kWh
		totalEnergy = self.ErrorDivide(totalEnergy, (3600000.0, 0.0))

		#Work out total g CO2
		massCO2: Tuple[float, float] = self.ErrorMultiply(self.totalMolesCO2, (self.CO2_MOLAR_MASS, 0.0))

		#Convert mass to tons
		massCO2 = self.ErrorDivide(massCO2, (1000000.0, 0.0))

		#Work out kWh per ton CO2
		powerConsumption = self.ErrorDivide(totalEnergy, massCO2)

		return powerConsumption


	def GetCO2Flux(self) -> Tuple[float, float]:
		#Get duration of relevant data window in s
		timeSeries: pd.Series = self.dataWindow["_time"].apply(self.ToUNIXTime)
		duration: float = timeSeries.iloc[timeSeries.size - 1] - timeSeries.iloc[0]

		#Begin arithmetic
		fluxCO2: Tuple[float, float] = (0.0, 0.0)

		#Work out total mass of CO2 evolved in g
		massCO2: Tuple[float, float] = self.ErrorMultiply(self.totalMolesCO2, (self.CO2_MOLAR_MASS, 0.0))
		#Convert mass to mg
		massCO2 = self.ErrorMultiply(massCO2, (1000.0, 0.0))

		#Work out CO2 evolution rate in mg/s
		rateCO2: Tuple[float, float] = self.ErrorDivide(massCO2, (duration, 0.0))

		#Work out total membrane area:
		totalArea: Tuple[float, float] = (self.MEMBRANE_PAIRS * self.MEMBRANE_AREA, 0.0)

		#Work out CO2 flux
		fluxCO2 = self.ErrorDivide(rateCO2, totalArea)

		return fluxCO2
