# skills-challenge
Experimenting with NOAA ISD Lite data

- To reproduce the experiment, either clone the repository or simply download the ftp_processor.py file
- Navigate to the directory on your local machine where the python file is located.
- Make sure there is a folder called 'data' in the same directory as the python file if you plan on adjusting the saveCSV parameter to True in the __main__ method.
- run python ftp_processor.py
- When the program is done running, open the max_temp_results and look at the 'annual_max_temp_by_satellite.csv' file to see the results.

*Note: No subprocesses or distributed computing was utilized to expedite the data extraction and manipulation. To examine the first n satellites listed in a directory for a given year, change the global param PCT_SATELLITES from 100 to some value between 1 and 100 to get experiment results on a subset of the total satellites.
