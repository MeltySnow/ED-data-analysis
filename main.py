#Import packages from pip
from typing import Type
import pandas as pd
import sys
import argparse

#Import project files
from ed_analysis_manager import EDAnalysisManager
import config_manager

#Configure argparse for handling command line arguments
parser: argparse.ArgumentParser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("experimentIDs", action="store", help="List of experiment IDs to include", nargs='*')
parser.add_argument("-o", "--output", action="store", help="Specify the name of the output file. Default is out.html")
parser.add_argument("-d", "--dashboard", action="store", help="Specify the ID of the Notion dashboard to read from")
parser.add_argument("-x", "--exclude", action="store_true", help="Processes all experiments marked as \"Completed\", excluding those supplied as positional arguments")
#parser.add_argument("-i", "--id-file", action="store", help="Pass the name of a file containing experiment IDs, each on a new line")
parser.add_argument("--config-gen", action="store_true", help="Generate a config file named ed_data_analysis.conf with all options set to their defaults")
parser.add_argument("-c", "--config", action="store", help="Specify the name of a config file from which configuration options will be loaded. Options set in this file will always be overridden by command line arguments")

#Actually parse command line arguments and convert from argparse.Namespace to dict
config: dict = vars(parser.parse_args())

#If the --config-gen flag is set, create the config file and exit
if config["config_gen"]:
	config_manager.ConfigGen()
	sys.exit(0)

#If the --config flag is set, load config options from the file
if config["config"]:
	config_manager.LoadConfig(config)

try:
	edAnalysis = EDAnalysisManager(config)
except Exception as e:
	print (e, file=sys.stderr)
	sys.exit(1)

try:
	edAnalysis.PlotData()
except Exception as e:
	print (e, file=sys.stderr)
	sys.exit(1)
