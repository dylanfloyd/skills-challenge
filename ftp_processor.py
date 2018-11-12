from ftplib import FTP
from math import floor
import pandas as pd
import numpy as np
import os
import time


ftp_url = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/'
PCT_SATELLITES = 100
col_names = ['YEAR',
             'MONTH',
             'DAY',
             'HOUR',
             'AIR-TEMP',
             'DEW-POINT-TEMP',
             'SEA-LEVEL-PRESSURE',
             'WIND-DIRECTION',
             'WIND-SPEED',
             'SKY-CONDITION',
             'RAIN-1HR', #liquid precipitation depth (measured in millimeters) over 1 hour
             'RAIN-6HR']

# login
# This method is used to instantiate an FTP object and navigate the .gov site to get to the noaa/isd-lite folder
def login():
    my_ftp = FTP('ftp.ncdc.noaa.gov')
    my_ftp.login()
    my_ftp.cwd('/pub/data/noaa/isd-lite/')
    return my_ftp

# changeFtpDirectory
# This method is used to navigate to a certain folder within the noaa/isd-lite ftp. Able to override inputs to handle
# a variety of directories and folders. In this case 'year' would be the new folder to navigate to.
def changeFtpDirectory(anFTP, year, newpath='/pub/data/noaa/isd-lite/'):
    newpath += str(year)
    anFTP.cwd(newpath)
    return None

# gz_to_df
# Used to import a .gz file from the FTP link and return a dataframe
# The pandas Dataframe handles uses whitespace as a delimeter to parse the text file
# saveCSV defaults to true and will save the df to a csv file locally under the year passed to the function
def gz_to_df(year, gz_filename, ftp_url_dir='ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/', saveCSV=True):
    afilepath = ftp_url_dir + str(year) + '/' + gz_filename
    print afilepath
    df = pd.read_csv(afilepath, compression='gzip', header=0, error_bad_lines=False, delim_whitespace=True)
    df.columns = col_names
    satinfo = gz_filename[0:len(gz_filename)-3]
    if saveCSV:
        save_csv(df, year, satinfo)
    return df


# dynamic_NaN_removal
# This method captures n columns with the fewest NA values
def dynamic_NaN_removal(df, n, verbose=False):
    nasum = df.isna().sum()
    nasum_np = np.array(nasum)
    if verbose:
        print nasum_np
    nasum_np.sort()
    na_threshold = int(nasum_np[n])
    valid_nasums = nasum[nasum < na_threshold]
    relevant_columns = list(valid_nasums.index)
    newdf = df.iloc[:, relevant_columns]
    return newdf


# save_csv
# This method saves a pandas dataframe to a local folder with a particular filename as a .csv file.
# If the directory does not already exist, it will create it. Most of the folders created were for a particular year
def save_csv(df, folder, filename="testsave"):
    directory = './data/'
    folder_directory = directory + str(folder)
    if not os.path.exists(folder_directory):
        os.makedirs(folder_directory)
    local_savepath = folder_directory + '/' + filename + '.csv'
    df.to_csv(local_savepath)


# collect_annual_data
# This method takes in the ftp object, and a start and stop year to import NOAA-ISD-Lite data, convert the .txt files
# to .csv files, and save them locally for further analysis if desired.
# Empty lists at the top can be used to store information read from the csv files so that the csv's don't have to be
# stored locally if the end user only wants to gather data in working memory rather than store it on the hard drive.
# Lists won't be returned until all of the files have been processed.
def collect_annual_data(anFtp, start_yr, end_yr, saveCSV=True):
    maxtemps = []
    process_times = []
    dates = []
    gzfilenames = []
    for year in range(start_yr, end_yr, 1):
        changeFtpDirectory(anFtp, year)
        dot_gzfiles = anFtp.nlst()
        max_satellites = max(1, int(floor(len(dot_gzfiles)*PCT_SATELLITES/100)))
        dot_gzfiles = dot_gzfiles[0:max_satellites]
        num_files = len(dot_gzfiles)
        i = 0
        for gzfilename in dot_gzfiles:
            i += 1
            try:
                runtime_msg = "processing " + str(i) + " of " + str(num_files) + ": " + str(gzfilename) + '...'
                print runtime_msg
                before = time.time()
                adf = gz_to_df(year, gzfilename, saveCSV=saveCSV)
                after = time.time()
                maxtemp = adf['AIR-TEMP'].max()
                index = adf['AIR-TEMP'].idxmax()
                maxtemps.append(maxtemp)
                runtime = after - before
                process_times.append(runtime)
                gzfilenames.append(gzfilename)
                month= int(adf.iloc[index, :]['MONTH'])
                day= int(adf.iloc[index, :]['DAY'])
                hour= int(adf.iloc[index, :]['HOUR'])
                dates.append(pd.datetime(year=int(year), month=month, day=day, hour=hour))
            except:
                runtime_msg = "Skipped the following file: " + str(gzfilename)
                print runtime_msg

    return [maxtemps, dates, process_times, gzfilenames]


#For each satellite, this function gets info on the day with the highest recorded temperature
def get_satellite_maxtemp_by_year(anFTP, year, saveCSV=True):
    output = collect_annual_data(anFTP, start_yr=year, end_yr=int(year)+1, saveCSV=saveCSV)
    # maxtemps, dates, process_times, gzfilenames = output
    return output



if __name__ == '__main__':
    print "Logging into FTP"
    ftp = login()
    ftp.cwd('/pub/data/noaa/isd-lite/2017')
    output = get_satellite_maxtemp_by_year(ftp, 2017, saveCSV=False) #change to True to reproduce csv files.
    maxtemps, dates, process_times, filenames = output

    df = pd.DataFrame()
    df['dates'] = dates
    df['filenames'] = filenames
    df['process_time'] = process_times
    df['maxtemp'] = maxtemps
    save_csv(df, 'max_temp_results', 'annual_max_temp_by_satellite')
