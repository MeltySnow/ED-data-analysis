from ed_analysis_manager import EDAnalysisManager
from typing import Type
import pandas as pd
import sys
import argparse

#Configure argparse for handling command line arguments
parser: argparse.ArgumentParser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("experimentIDs", action="store", help="List of experiment IDs to include", nargs='*')

config: argparse.Namespace = parser.parse_args()

try:
	edAnalysis = EDAnalysisManager(config.experimentIDs)
except Exception as e:
	print (e, file=sys.stderr)
	sys.exit(1)

edAnalysis.PlotData()
