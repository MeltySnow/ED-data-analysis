from ed_analysis_manager import EDAnalysisManager
from typing import Type
import pandas as pd
import sys
import argparse

#Configure argparse for handling command line arguments
parser: argparse.ArgumentParser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("experimentIDs", action="store", help="List of experiment IDs to include", nargs='*')
parser.add_argument("-o", "--output", action="store", help="Specify name out output file. Default is out.html")
parser.add_argument("-d", "--dashboard", action="store", help="Specify the ID of the Notion dashboard to read from")
parser.add_argument("-x", "--exclude", action="store_true", help="Processes all experiments marked as \"Completed\", excluding those supplied as positional argiments")

config: argparse.Namespace = parser.parse_args()

try:
	edAnalysis = EDAnalysisManager(config)
except Exception as e:
	print (e, file=sys.stderr)
	sys.exit(1)

edAnalysis.PlotData()
