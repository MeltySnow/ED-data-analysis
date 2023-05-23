from typing import Type, Tuple
import pandas as pd
import sys
import ed_constants

def ErrorDivide(a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
	if len(a) != 2 or len(b) != 2:
		raise Exception("Tuple not in the right format. Should be (value, error") 

	outputValue = a[0] / b[0]
	relativeError = (a[1]/a[0]) + (b[1]/b[0]) 
	outputError = outputValue * relativeError 
	return (outputValue, outputError)

def ErrorMultiply(a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
	if len(a) != 2 or len(b) != 2:
		raise Exception("Tuple not in the right format. Should be (value, error") 

	outputValue = a[0] * b[0]
	relativeError = (a[1]/a[0]) + (b[1]/b[0]) 
	outputError = outputValue * relativeError 
	return (outputValue, outputError)


#In all these functions, numbers are stored as tuples of format (data, error)
def GetCurrentDensity(dataWindow: pd.DataFrame) -> Tuple[float, int]:
	#Parses current densities into well-defined categories to use as dictionary keys
	#This code is VERY DODGY and makes it non-portable to even vaguely different experimental methodologies
	#However I am too dumb to think of a smarter way
	#Don't do magic numbers, kids. Magic numbers are bad
	currentDensities: List[float] = [120.0, 200.0, 280.0, 360.0, 440.0, 540.0]

	#Calculate ACTUAL current density
	#Extract current from dataframe:
	actualCurrentDensity: float = dataWindow["current_PSU001"].mean() / ed_constants.MEMBRANE_AREA


	#Now we see which key the actual value is closest to
	outputIndex: int = -1
	for n in range(0, len(currentDensities)):
		if outputIndex == -1:
			outputIndex = n
		else:
			diff: float = abs(currentDensities[n] - actualCurrentDensity)
			best: float = abs(currentDensities[outputIndex] - actualCurrentDensity)
			if diff < best:
				outputIndex = n
	return (actualCurrentDensity, int(currentDensities[outputIndex]))


def GetStackResistance(dataWindow: pd.DataFrame) -> Tuple[float, float]:
	#Extract values and errors from dataframe:
	current: Tuple[float, float] = (dataWindow["current_PSU001"].mean(), dataWindow["current_PSU001"].std())
	voltage: Tuple[float, float] = (dataWindow["voltage_PSU001"].mean(), dataWindow["voltage_PSU001"].std())

	#Perform arithmetic
	resistance = (0.0, 0.0)
	try:
		resistance = ErrorDivide(voltage, current)
	except Exception as e:
		print (e, file=sys.stderr)
		raise Exception("Warning: there was an error when calculating stack resistance")
	return resistance

def GetCurrentEfficiency(dataWindow: pd.DataFrame) -> Tuple[float, float]:
	#Extract values and errors from dataframe:
	#percentCO2: Tuple[float, float] = ((dataWindow["CO2_PPM_CO2001"].mean()/10000.0), (dataWindow["CO2_PPM_CO2001"].std()/10000.0))
	fractionCO2: Tuple[float, float] = ((dataWindow["CO2_PPM_CO2001"].mean()/1000000.0), (dataWindow["CO2_PPM_CO2001"].std()/1000000.0))
	airVolumetricFlow: Tuple[float, float] = (dataWindow["volumetric_flow_MFM001"].mean(), dataWindow["volumetric_flow_MFM001"].std())
	current: Tuple[float, float] = (dataWindow["current_PSU001"].mean(), dataWindow["current_PSU001"].std())

	#Begin arithmetic
	currentEfficiency: Tuple[float, float] = (0.0, 0.0)
	try:
		#Convert volumetric flow from L min^{-1} to L s^{-1}
		airVolumetricFlow = ErrorDivide(airVolumetricFlow, (60.0, 0.0))
		#Work out number of mol of CO2 produced per second:
		molCO2 = ErrorMultiply(fractionCO2, airVolumetricFlow)
		molCO2 = ErrorMultiply(molCO2, (ed_constants.CO2_DENSITY, 0.0))
		molCO2 = ErrorDivide(molCO2, (ed_constants.CO2_MOLAR_MASS, 0.0))
		#Work out number of mol of electrons passed per second:
		molElectrons = ErrorDivide(current, (ed_constants.FARADAY_CONSTANT, 0.0))
		#Work out mol of CO2 per mol of e-
		currentEfficiency = ErrorDivide(molCO2, molElectrons)
		#Convert to %
		currentEfficiency = ErrorMultiply(currentEfficiency, (100.0, 0.0))
		#Work out CE per cell pair
		currentEfficiency = ErrorDivide(currentEfficiency, (ed_constants.MEMBRANE_PAIRS, 0.0))
		#print ("CO2 mol: %f\ne- mol: %f" % (molCO2[0], molElectrons[0]))
	except Exception as e:
		currentEfficiency = (0.0, 0.0)
		print (e, file=sys.stderr)
		raise Exception("Warning: there was an error when calculating current efficiency")
	return currentEfficiency	


def GetPowerConsumption(dataWindow: pd.DataFrame) -> Tuple[float, float]:
	#Extract values and errors from dataframe:
	current: Tuple[float, float] = (dataWindow["current_PSU001"].mean(), dataWindow["current_PSU001"].std())
	voltage: Tuple[float, float] = (dataWindow["voltage_PSU001"].mean(), dataWindow["voltage_PSU001"].std())
	fractionCO2: Tuple[float, float] = ((dataWindow["CO2_PPM_CO2001"].mean()/1000000.0), (dataWindow["CO2_PPM_CO2001"].std()/1000000.0))
	airVolumetricFlow: Tuple[float, float] = (dataWindow["volumetric_flow_MFM001"].mean(), dataWindow["volumetric_flow_MFM001"].std())

	#Begin arithmetic
	powerConsumption: Tuple[float, float] = (0.0, 0.0)
	try:
		#Work out power in W:
		power: Tuple[float, float] = ErrorMultiply(current, voltage)
		#Convert power to kWh s^{1-}
		power = ErrorDivide(power, (3600000.0, 0.0))

		#Convert volumetric flow from L min^{-1} to L s^{-1}
		airVolumetricFlow = ErrorDivide(airVolumetricFlow, (60.0, 0.0))

		#Work out g CO2 s^{-1}
		massCO2: Tuple[float, float] = ErrorMultiply(fractionCO2, airVolumetricFlow)
		massCO2 = ErrorMultiply(massCO2, (ed_constants.CO2_DENSITY, 0.0))
		#Convert mass to tons
		massCO2 = ErrorDivide(massCO2, (1000000.0, 0.0))

		#Work out kWh per ton CO2
		powerConsumption = ErrorDivide(power, massCO2)

	except Exception as e:
		powerConsumption = (0.0, 0.0)
		print (e, file=sys.stderr)
		raise Exception("Warning: there was an error when calculating power consumption")
	return powerConsumption


def GetCO2Flux(dataWindow: pd.DataFrame) -> Tuple[float, float]:
	#Extract values and errors from dataframe:
	fractionCO2: Tuple[float, float] = ((dataWindow["CO2_PPM_CO2001"].mean()/1000000.0), (dataWindow["CO2_PPM_CO2001"].std()/1000000.0))
	airVolumetricFlow: Tuple[float, float] = (dataWindow["volumetric_flow_MFM001"].mean(), dataWindow["volumetric_flow_MFM001"].std())


	#Begin arithmetic
	fluxCO2: Tuple[float, float] = (0.0, 0.0)
	try:
		#Convert volumetric flow from L min^{-1} to L s^{-1}
		airVolumetricFlow = ErrorDivide(airVolumetricFlow, (60.0, 0.0))

		#Work out g CO2 s^{-1}
		massCO2: Tuple[float, float] = ErrorMultiply(fractionCO2, airVolumetricFlow)
		massCO2 = ErrorMultiply(massCO2, (ed_constants.CO2_DENSITY, 0.0))
		#Convert mass to mg
		massCO2 = ErrorMultiply(massCO2, (1000.0, 0.0))

		#Work out CO2 flux:
		totalArea: float = (ed_constants.MEMBRANE_PAIRS * ed_constants.MEMBRANE_AREA, 0.0)
		fluxCO2 = ErrorDivide(massCO2, totalArea)
	except Exception as e:
		fluxCO2 = (0.0, 0.0)
		print (e, file=sys.stderr)
		raise Exception("Warning: there was an error when calculating CO2 flux")
	return fluxCO2
