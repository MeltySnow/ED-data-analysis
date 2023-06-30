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
python3 [options]  main.py
```
Default behaviour is to pull metadata from every experiment in the [notion dashboard](https://notion.so/mzt/Capture-Exp-Plan-Raw-Data-54334d792f0545b08377c7f4221d48b0) with the "Completed" column ticked. Experiments will be sorted into chronological order.
If passed any positional arguments, the program wil search the notion dashboard's "Experimental Name" column for IDs matching the command line arguments, and pull only those entries for analysis. When postional arguments are given, the experiments will not be sorted, and will appear in the graph in the order that they are given. For example:
```
python3 main.py AS_ED_01 AS_ED_02 AS_ED_07
```
Options are as follows:
```
-h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Specify the name of the output file. Default is out.html
                        (default: None)
  -d DASHBOARD, --dashboard DASHBOARD
                        Specify the ID of the Notion dashboard to read from
                        (default: None)
  -x, --exclude         Processes all experiments marked as "Completed", excluding
                        those supplied as positional arguments (default: False)
  --config-gen          Generate a config file named ed_data_analysis.conf with all
                        options set to their defaults (default: False)
  -c CONFIG, --config CONFIG
                        Specify the name of a config file from which configuration
                        options will be loaded. Options set in this file will
                        always be overridden by command line arguments (default:
                        None)
```
If the script runs successfully, it will produce a file named `out.html` by default which contains the rendered figures.
