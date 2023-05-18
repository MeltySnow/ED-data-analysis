from ed_analysis_manager import EDAnalysisManager
from typing import Type
import pandas as pd
import sys

argc: int = len(sys.argv)

if argc <= 1:
	print ("No arguments given. Please provide the desired experiment ID")
	sys.exit(1)

try:
	tst = EDAnalysisManager(argc, sys.argv) 
except Exception as e:
	print (e, file=sys.stderr)
	sys.exit(1)
