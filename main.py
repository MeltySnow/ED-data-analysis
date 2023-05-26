from ed_analysis_manager import EDAnalysisManager
from typing import Type
import pandas as pd
import sys

try:
	edAnalysis = EDAnalysisManager(len(sys.argv), sys.argv) 
except Exception as e:
	print (e, file=sys.stderr)
	sys.exit(1)
