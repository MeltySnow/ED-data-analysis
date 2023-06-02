# Overview
Python script for automatically processing data logged from Mission Zero Technologies' electrodialysis (ED) stands. Functionality covers:
- Downloading experiment metadata from a Notion dashboard
- Querying raw experimental data from InfluxDB
- Performing arithmetic to extract key performance metrics from the raw data
- Drawing interactive figures using Plotly

# Dependencies
python3 is required to run this script. It is recommended that you use the latest version of Python. The script has been tested and confirmed to work on versions 3.8 and 3.11.

A `requirements.txt` file is provided. It is recommended that you install the required packages to a virtual environment using pip. On a UNIX-like system, this can be done by executing the following commands inside the cloned directory:
```
python3 -m venv venv
source ./venv/bin/activate
python3 -m pip install -r requirements.txt
```

Additionally, a `.env` file is required to run the script. This is not hosted on GitHub as it contains API secrets. If you need to run the script, ask me for a copy of the `.env` file.

# Use
The script can be run using the command:
```
python3 main.py
```
Default behaviour is to pull metadata from every experiment in the [notion dashboard](https://notion.so/mzt/Capture-Exp-Plan-Raw-Data-54334d792f0545b08377c7f4221d48b0) with the "Completed" column ticked.
However the script also accepts command line arguments. If passed any, it will search the notion dashboard's "Experimental Name" column for IDs matching the command line arguments, and pull only those entries for analysis. It is recommended to use command line arguments as the resulting figures will be less cluttered, and this method allows the user to order the experiments sensibly (as Notion databases are not ordered). For example:
```
python3 main.py AS_ED_01 AS_ED_02 AS_ED_07
```
If the script runs successfully, it will produce a file named `out.html` which contains the rendered figures.