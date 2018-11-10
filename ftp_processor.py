import ftplib
from ftplib import FTP
import pandas as pd
import numpy as np
import os
import time

ftp_url = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/'
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
             'RAIN-1HR', #liquid precipitation depth (measured in millimeters
             'RAIN-6HR']



def changeFtpDirectory(anFTP, year, newpath='/pub/data/noaa/isd-lite/'):
    newpath += str(year)
    anFTP.cwd(newpath)
    return None



def gz_to_df(year, gz_filename, ftp_url_dir='ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/'):
    afilepath = ftp_url_dir + str(year) + '/' + gz_filename
    # print afilepath
    df = pd.read_csv(afilepath, compression='gzip', header=0, error_bad_lines=False, warn_bad_lines=False,
                     delim_whitespace=True)
    # df.columns = range(1, df.shape[1]+1)
    df.columns = col_names
    satinfo = gz_filename[0:len(gz_filename)-3]
    # local_savepath = './data/' + str(year) + '/' + satinfo + '.csv'
    save_csv(df, year, satinfo)
    return df


def get_multiple_dfs(year, gzfilenames=list(), ftp_url_dir='ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/'):
    dataframes = []
    counter = 1
    numfiles = len(gzfilenames)
    for gzfile in gzfilenames:
        try:
            satellite_info = gzfile[0:len(gzfile)-3]
            retrieving = 'retrieving ' + str(counter) + ' of ' + str(numfiles) + '...'
            print(retrieving)
            df = gz_to_df(year, gzfile)
            df = dynamic_NaN_removal(df)
            df.columns = col_names
            df['SATELLITE-INFO'] = satellite_info
            # print("dataframe preview:")
            # print(df.head())
            # print("shape: ", df.shape)
            # save_csv(df)
            dataframes.append(df)
            counter += 1
        except:
            err_msg = "error occured at: " + str(satellite_info)
            print err_msg
    return dataframes

#There's supposed to only be 12 relevant columns, but the delimitations in the text files are gross
#This method captures the 12 columns with the fewest NA values
def dynamic_NaN_removal(df):
    nasum = df.isna().sum()
    nasum_np = np.array(nasum)
    nasum_np.sort()
    threshold = int(nasum_np[12])
    nasum_t = nasum[nasum < threshold]
    relevant_columns = list(nasum_t.index)
    newdf = df.iloc[:, relevant_columns]
    return newdf


def save_csv(df, year, satinfo=""):
    directory = './data/'
    year_directory = directory + str(year)
    if not os.path.exists(year_directory):
        os.makedirs(year_directory)
    if satinfo == "":
        satinfo = df.iloc[0, -1]
    local_savepath = './data/' + str(year) + '/' + satinfo + '.csv'
    # filepath = './data/' + str(satinfo) + '.csv' #add a folder for year?
    df.to_csv(local_savepath)




def login():
    my_ftp = FTP('ftp.ncdc.noaa.gov')
    my_ftp.login()
    my_ftp.cwd('/pub/data/noaa/isd-lite/')
    return my_ftp









#next step is to get a handful of csvs from each year for some set of satellites
#have them save original dato the folders
#do something cool with these DFsj
def collect_annual_data(anFtp, start_yr, end_yr):
    maxtemps = []
    process_times = []
    dataframes = []
    dates = []
    gzfilenames = []
    for year in range(start_yr, end_yr, 1):
        changeFtpDirectory(anFtp, year)
        dot_gzfiles = anFtp.nlst()
        for gzfilename in dot_gzfiles[0:2]:
            runtime_msg = "processing " + str(gzfilename) + '...'
            print runtime_msg
            before = time.time()
            adf = gz_to_df(year, gzfilename)
            after = time.time()
            print adf.head()
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

            # dataframes.append(adf)
    return [maxtemps, dates, process_times, gzfilenames]


#For each satellite, this function gets info on the day with the highest recorded temperature
def get_hottest_day_in_year(anFTP, year):
    output = collect_annual_data(anFTP, start_yr=year, end_yr=int(year)+1)
    maxtemps, dates, process_times, gzfilenames = output
    # results_df = pd.DataFrame(data=[maxtemps, runtimes, dates], index=filenames)
    # return results_df
    return output


# ftp = FTP('ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/')
# ftp = FTP('ftp.ncdc.noaa.gov')
# ftp.login()
# ftp.dir()
# ftp.cwd('/pub/data/noaa/isd-lite')
# print "-"*30
# ftp.dir()
#
# ftp.cwd('/pub/data/noaa/isd-lite/2016')
# print "-"*30
# ftp.dir()

##################uncomment below
print "Logging into FTP"
ftp = login()
ftp.cwd('/pub/data/noaa/isd-lite/2016')
# dot_gzfiles = ftp.nlst()
# adf = gz_to_df(2016, dot_gzfiles[0])
output = get_hottest_day_in_year(ftp, 2016)
maxtemps, dates, process_times, filenames = output
df = pd.DataFrame()
df['dates'] = dates
df['filenames'] = filenames
df['process_time'] = process_times
df['maxtemp'] = maxtemps
save_csv(df, 'max_temp_results', 'annual_max_temp_by_satellite')

##################uncomment above


# reduced_df = dynamic_NaN_removal(adf)
# dfs2017 = get_multiple_dfs(2017, dot_gzfiles)
# get_multiple_dfs(2017, dot_gzfiles[0::100])
# dfs = get_multiple_dfs(2016, dot_gzfiles[0:2])
