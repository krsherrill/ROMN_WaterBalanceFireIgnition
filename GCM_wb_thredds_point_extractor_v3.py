#Mike Tercek, april 2020 - Version 0.5
#This script will download data from the NPS Water Balance Thredds Server located at http://www.yellowstone.solutions/thredds/catalog.html
#It reads a table of points located in the file called "my_gcm_points.csv"
#An example of how to format this file is included with this script. It should be located in the same folder as the script when you run the code.
#The script will get the data for all the years from first_year to last_year (see declarations below) and merge them into a single file. It will also fix the timestamps on the monthly data.
#Questions: Write to Tercek@YellowstoneEcology.com

######################################
#Modified By: Kirk Sherrill, April 2nd 2021
#Script Name - 'GCM_wb_thredds_point_extractor_v2.py
#Added functionality to compile data into one output .csv file across all defined sites (as define in: in mypoints.csv), parameters, GCM Models and RCP scenarios as defined in the
# 'mypointsFile' .csv setup file
#The compiled output .csv file will in the 'Merged_All' directory, down directory from
#the python script (i.e. 'GCM_wb_thredds_point_extractor_v2_{Date}.py), with the name defined in the 'outFileName' parameter of the script setup


#Modified By: Kirk Sherrill, October 12th, 2021
#Script Name - 'GCM_wb_thredds_point_extractor_v3.py
#Apply correction factor to take data from the mm * 10 multiplier (for integer storge) by dividing by 10 to remove the integer corretion variable.


# mypointsFile - variable defines the path and file Name to the .csv file defining the sites, lat/lon and water balance variables to be processed. (ie. the 'mypoints.csv' file)
#outFileName - variable defines the output name for the final .csv file with the compiled site, parameters and years of data.

# Software/Libraries: Python Version 3.x, Pandas Library

import csv, urllib, os, time, pandas as pd, glob, traceback, sys, socket, time, shutil
import urllib.request

def tryfloat(v):
    try:
        out = float(v)
    except:
        out = 'nan'
    return out

def make_neg(v):
    try:
        v = float(v)
    except:
        return 'nan'
    if v >= 0: v = v * -1
    return v

def read_point_file(mypointsFile):
    #fill_list = ['lat','lon']
    out_d = {}
    infile = open(mypointsFile,'rt')
    reader = csv.reader(infile)
    for line in reader:
        try:
            if '#' in line[0]: continue
            if line[0].strip() == 'Name' : continue
            name = line[0].strip()
            param = line[1].strip().lower()
            d_or_m = line[2].strip().lower()
            model = line[5]
            scenario = line[6]
        except:
            continue
        #place_name = name + '_' + param + '_' + d_or_m + '_' + model + '_' + scenario
        place_name = name + '_' + d_or_m + '_' + param + '_' + model + '_' + scenario #KRS Modified 20210402
        out_d[place_name] = {}
        out_d[place_name]['lat'] = tryfloat(line[3])
        out_d[place_name]['lon'] = make_neg(tryfloat(line[4]))
        out_d[place_name]['d_or_m'] = d_or_m
        out_d[place_name]['param'] = param
        out_d[place_name]['model'] = model
        out_d[place_name]['scenario'] = scenario
        out_d[place_name]['para_model_scenario'] = param + '_' + model + '_' + scenario  #Added KRS 20210323

    infile.close()
    return out_d

def get_one(d,place, paraModelScenarioList):
    global first_year, last_year, param_dict, daily_url, monthly_url
    print('Getting data for : ', place)
    lat = d[place]['lat']
    lon = d[place]['lon']
    param = d[place]['param']
    if param in param_dict: fparam = param_dict[param]
    else: fparam = param

    # KRS Added 20200609
    filepath = os.path.dirname(os.path.abspath(__file__))

    # Create Site Monthly Folder
    filepathSiteMonthly = filepath + "\\SiteMonthlyDailyFiles"
    if os.path.exists(filepathSiteMonthly) == True:
        print("Directory - " + filepathSiteMonthly + " Exists")
    else:
        os.makedirs(filepathSiteMonthly)

    # KRS Added 20200609
    d_or_m = d[place]['d_or_m']
    model = d[place]['model']
    scenario = d[place]['scenario']
    param_model_scenario = d[place]['para_model_scenario']  #KRS Added 20210323

    if param_model_scenario not in paraModelScenarioList:
        paraModelScenarioList.append(param_model_scenario)


    #print('~',place)
    check_name = place +  '_all_years.csv'
    #print(check_name)
    fl = os.listdir('.')
    if check_name in fl: return False
    for year in range(first_year,last_year + 1):
        print(year)
        next_year = year + 1
        if d_or_m == 'daily' : this_url = daily_url.format(scenario = scenario, model = model, year = year, param = fparam, lon = lon, lat = lat, next_year = next_year)
        else: this_url = monthly_url.format(scenario = scenario, model = model, year = year, param = fparam, lon = lon, lat = lat, next_year = next_year, param_lower = fparam.lower())

        fullSiteMonthly = filepathSiteMonthly + "\\" + place + '_' + str(year) + '.csv'  # KRS Added 20200609

        #print(this_url)
        remaining_download_tries = 5
        success = False
        while remaining_download_tries > 0:
            try:
                #print(this_url)
                #urllib.request.urlretrieve(this_url, place + '_' + str(year) + '.csv')
                urllib.request.urlretrieve(this_url, fullSiteMonthly)
                remaining_download_tries = 0
                success = True
            except:
                print('Trying again: ', year)
                remaining_download_tries = remaining_download_tries - 1
                time.sleep(3)


        if success == True:    #Import the created file and rename the parameter field with the 'model_scenario' value - KRS

            #Rename output field with Model and RCP info: {param_model_scenario}
            df = pd.read_csv(fullSiteMonthly)
            shapeOutput = df.shape
            lastColumn = int(shapeOutput[1]) - 1
            lastColumnName = df.columns[lastColumn]
            df.rename(columns = {lastColumnName:param_model_scenario},inplace=True)
            # Remove * 10 correction for integer   - Added by KRS 20211012
            df[param_model_scenario] = df[param_model_scenario] / 10.0

            fullSiteMonthly = filepathSiteMonthly + "\\" + place + '_' + str(year) + '.csv'
            os.remove(fullSiteMonthly) #Delete Initial File
            df.to_csv(fullSiteMonthly, ",") #Save modified .csv with para_model_scenario field

        if success == False: break


    return True
        
def fix_monthly_lines(line,year):
     #line = line.replace('1980-',str(year) + '-')
     pl = line.split(',')
     ts = pl[0]
     ts = ts[:7]
     pl[0] = ts
     outline = ','.join(pl)
     return outline


def merge_years(d, place):
    global first_year, last_year
    print('Merging year files for :', place)
    # KRS Added 20200609
    filepath = os.path.dirname(os.path.abspath(__file__))  # KRS Added 20190702
    filepathMergedYears = filepath + "\\MergeYearFiles"
    if os.path.exists(filepathMergedYears) == True:

        print("Directory - " + filepathMergedYears + " - Exists")
    else:
        os.makedirs(filepathMergedYears)

    # outfilename = place + '_all_years.csv'
    outfilename = filepathMergedYears + "\\" + place + '_all_years.csv'
    outfile = open(outfilename, 'w')
    filepathSiteMonthly = filepath + "\\SiteMonthlyDailyFiles"  # Site Monthly File Directory
    for year in range(first_year, last_year + 1):
        infilename = place + '_' + str(year) + '.csv'
        infilenameFull = filepathSiteMonthly + "\\" + infilename  # KRS Added 20200609

        infile = open(infilenameFull, 'rt')  # Modified path - KRS Added 20200609

        fileNameSplit = infilename.split("_")  # Split infileName to get at the SiteName
        if siteNameUnderscore == 'Yes':
            siteNameStr = fileNameSplit[0] + "_" + fileNameSplit[1]  # Define siteName
        else:
            siteNameStr = fileNameSplit[0]

        df = pd.read_csv(infilenameFull)  # Push csv to dataframe so 'SiteName' field can be appended
        df.insert(0, "SiteName",
                  siteNameStr)  # Add the 'SiteName' field with accompanying sitename'

        df.drop(["Unnamed: 0"], axis=1, inplace=True)
        outputCSVwSiteName = infilenameFull  # Overwrite Exists Site Yearly File with dataframe with 'SiteName' field added
        df.to_csv(outputCSVwSiteName, ",", index=False)

        # KRS Added 20200609

        line_count = 1
        for line in infile:
            if year != first_year and line_count == 1:
                line_count += 1
                continue
            if 'monthly' in infilename: line = fix_monthly_lines(line, year)
            outfile.write(line)
            line_count += 1
        infile.close

    outfile.close()

# Function Merges Monthly Files For All Sites by Parameter - KRS Added 20200609
def mergeMonthly_sites(parameterList, paraModelScenarioList):
    # Create 'FinalMergedParameter' directory for csv files merged by parameter across all sites and years
    filepath = os.path.dirname(os.path.abspath(__file__))
    filepathFinalMergedParameter = filepath + "\\MergedParameter"
    if os.path.exists(filepathFinalMergedParameter) == True:

        print("Directory Exists - " + filepathFinalMergedParameter)

    else:
        os.makedirs(filepathFinalMergedParameter)

    # Add parameter loop here - creating one .csv per parameter across all sites/years for the respective Parameter, GCP Model and RCP Scenario as defined in paraMdelScenarioList
    for parameter in paraModelScenarioList:

        filepath = os.path.dirname(os.path.abspath(__file__))
        filepathMergedYears = filepath + "\\MergeYearFiles"
        filesMonthlyList = glob.glob(filepathMergedYears + "\\*" + parameter + "*.csv")

        #########Routine for Monthly Files
        print('Merging Files for parameter - ' + parameter)

        outfilename = 'MergedSiteYear_' + parameter + '.csv'
        outfile = open(filepathFinalMergedParameter + "\\" + outfilename, 'w')
        fileCount = 1
        for file in filesMonthlyList:
            print(file)
            infile = open(file, 'rt')
            line_count = 1
            for line in infile:
                if fileCount > 1 and line_count == 1:  # Skip Header after initial file
                    line_count += 1
                    continue

                # if 'monthly' in infilename: line = fix_monthly_lines(line, year)
                outfile.write(line)
                line_count += 1
            infile.close
            fileCount += 1

        outfile.close()


#calculate the ensemble mean for the defined RCP in variable 'calEnsembleAvg'
def define_EnsembleMean(dfIn, parameterList, calEnsembleAvg):
    try:
        #Calculate ensemble averages per parameter
        for parameter in parameterList:
            #Calculate ensembles per RCP
            for rcp in calEnsembleAvg:
                # Empty List of Fields to Process
                fieldList = []
                # loop thru fields in dataframe - define fields to average
                for col in dfIn.columns:
                    #Check for RCP in field name
                    if rcp in col:
                        #Check for Parameter in field name
                        if parameter in col:
                            # Append field if matches
                            fieldList.append(col)
                            print(col)

                #If fieldList len = 0 skip
                if len(fieldList) == 0:
                    continue

                # Define Output Name
                outField = parameter + "_Ensemble_" + rcp

                #Calculate the Ensemble Mean - to new field
                dfIn[outField] = dfIn[fieldList].mean(axis=1)

        return "Success function", dfIn

    except:
        messageTime = timeFun()
        print("Error on define_EnsembleMean Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'define_EnsembleMean'"


#Function Merges all the Merged parameter files (across all sites and years) in the 'MergedParameter' directory to
#one file 'MergedAll.csv' in directory 'MergedAll'. Variables that are processed are defined in the 'parameterList' variable
def mergeFinalparameterList(paraModelScenarioList, outFileName):
    # Create 'FinalMergedParameter' directory for csv files merged by parameter across all sites and years
    filepath = os.path.dirname(os.path.abspath(__file__))  # KRS Added 20190702
    filepathFinalMerged = filepath + "\\MergedAll"
    if os.path.exists(filepathFinalMerged) == True:

        print("Directory - " + filepathFinalMerged + " - Exists")
    else:
        os.makedirs(filepathFinalMerged)

    #Loop thru the parameters that have been collected for the respective Parameter, GCP Model and RCP Scenario as defined in paraMdelScenarioList
    parameterCount = 1
    for parameter in paraModelScenarioList:

        filepathFinalMergedParameter = filepath + "\\MergedParameter"
        filesList = glob.glob(filepathFinalMergedParameter + "\\*" + parameter + "*.csv")

        if len(filesList)> 1:
            print ('More than One File for defined parameter - ' + parameter + ' - Exiting Script')
            sys.exit()

        inTable = filesList[0] #Table to be processed

        if parameterCount == 1: #Create initial data frame to which all variables will be pushed to
            df = pd.read_csv(inTable)
            parameterCount += 1
            os.remove(inTable)
            continue

        else:   #Merge the next parameter variable via key fields 'SiteName','time'

            df2 = pd.read_csv(inTable) #Load the next parameter file to dataframe 2
            # Get list of columns in the dataframe
            columnList = list(df2)
            latColumn = 'latitude[unit="degrees_north"]' #Column three should be Lat
            lonColumn = 'longitude[unit="degrees_east"]' #Column four should be Lon
            df2.drop([latColumn, lonColumn], axis=1, inplace=True)  #Drop the lat and lon columns prior to merging
            print(list(df2))

            if parameterCount == 2:
                mergeCurrent = df.merge(df2, on=['SiteName', 'time'], how='left')
            else:
                mergePrevious = mergeCurrent
                del mergeCurrent
                mergeCurrent = mergePrevious.merge(df2, on=['SiteName', 'time'], how='left')

            print(list(mergeCurrent))

            print("Successfully Merged Parameter - " + parameter + " to compiled dataframe")
            del df2

        parameterCount += 1


    #Calculate the Ensemble Mean per parameter
    outVal = define_EnsembleMean(mergeCurrent, parameterList, calEnsembleAvg)
    if outVal[0] != "Success function":
        print("WARNING - Function define_EnsembleMean failed - Exiting Script")
        exit()
    else:
        print("Success - Function define_EnsembleMean")
        #Pass Dataframe
        mergeCurrent = outVal[1]



    mergeCurrent.to_csv(filepathFinalMerged + "\\" + outFileName + ".csv", ",", index = False)
    del df
    del mergeCurrent
    messageTime = timeFun()
    print("Exported Final Merged .csv file to - " + filepathFinalMerged + "\\" + outFileName + ".csv - " + messageTime)

def timeFun():          #Function to Grab Time
    from datetime import datetime
    b=datetime.now()
    messageTime = b.isoformat()
    return messageTime


def unit_fix(line):
    pl = line.split(',')
    #try:
    val = float(pl[3])
    if val < - 30000 : bad_line = True  
    else: bad_line = False
    #except:
        #bad_line = True
    val = val / 10 # Units converted back to mm from gcm data, which has mm * 10
    pl[3] = str(val)
    new_line = ','.join(pl)
    new_line = new_line + '\n'
    return new_line, bad_line

def header_fix(line):
    pl = line.split(',')
    pl[3] = 'extracted data'
    new_line = ','.join(pl)
    new_line = new_line + '\n'
    return new_line

        
if __name__ == '__main__': 
    import os, shutil

    parameterList = ['soil_water', 'runoff', 'agdd', 'pet', 'deficit', 'rain', 'accumswe', 'aet']  #List of Parameters to be processed
    #parameterList = ['deficit']

    first_year = 2020
    #last_year = 2099
    last_year = 2021

    calEnsembleAvg = ['rcp45','rcp85']    #Ensemble Average to derive ('No'|'rcp45'|'rcp85') respectively for No Ensemble, 4.5 and 8.5 emissions scenarios.
    siteNameUnderscore = "Yes"  # Variable defines if the site name has an underscore (e.g FLFO_001) ('Yes'|'No'), used to define the 'Site' field from the file name
    mypointsFile = r'C:\ROMN\Climate\ClimateAnalyzer\Dashboards\ROMO\GridMetStations\bearlake_from_grid\my_gcm_points_DeficitOnly_bearlake_from_grid_Test.csv'  # path and File Name used to define the points to be processed - in the same format as 'mypoints.csv'
    outFileName = "bearlake_fromgrid_futures"  # Output File name for the .csv file with all Sites, Parameters and Years merged into one file

    deleteDirectories = "Yes"  # If set to yes the 'SiteMonthlyDailyFiles','MergeYearFiles', and 'MergeParameter' directories will be deleted at the start of processing- To avoid processing previous data.


    ######################
    # Hard Coded Below
    ######################
    param_dict = {'aet': 'AET', 'pet': 'PET', 'deficit': 'Deficit'}

    monthly_url = 'http://www.yellowstone.solutions/thredds/ncss/daily_or_monthly/gcm/{scenario}/{model}/V_1_5_{year}_{model}_{scenario}_{param}_monthly.nc4?var={param_lower}&latitude={lat}&longitude={lon}&time_start={year}-01-16T05%3A14%3A31.916Z&time_end={next_year}-12-17T00%3A34%3A14.059Z&accept=csv_file'
    daily_url = 'http://www.yellowstone.solutions/thredds/ncss/daily_or_monthly/gcm/{scenario}/{model}/V_1_5_{year}_{model}_{scenario}_{param}.nc4?var={param}&latitude={lat}&longitude={lon}&time_start={year}-01-01T00%3A00%3A00Z&time_end={next_year}-01-01T00%3A00%3A00Z&accept=csv_file'

    # KRS Added 20210319
    model_scenario_List = []  # As model and scenario list are processed this list will be populated
    filepath = os.path.dirname(os.path.abspath(__file__))
    # Delete Existing Directories that might have files from previous processing.
    if deleteDirectories.lower() == "yes":

        dirList = ["SiteMonthlyDailyFiles", "MergeYearFiles", "MergedParameter"]
        for directory in dirList:
            fullPath = filepath + "\\" + directory
            if os.path.exists(fullPath):
                shutil.rmtree(filepath + "\\" + directory)
                print("Deleted directory -" + filepath + "\\" + directory)

    #############

    paraModelScenarioList = []  #List to hold all Parameter, Model, Rcp scenario's used in the 'mergeMonthly_sites' function
    d = read_point_file(mypointsFile)
    for place in d:
        new = get_one(d, place, paraModelScenarioList)
        if new == True:
            try:
                merge_years(d, place)
            except:
                continue
        else:
            print('Skipping, duplicate: ', place)

    ##Function to Merged the files in 'MergeYearFiles (One Compiled CSV files by Across All Sites and Years for all Parameter, Model, and RCP Scenarios processed
    # OutputFolder: MergedParameter
    outMerge_sites = mergeMonthly_sites(parameterList, paraModelScenarioList)

    # Function to Merged the files in 'MergedParameter (i.e. the Compiled CSV files by Parameter ') into one fully compiled (All Sites/Years/Parameters)
    # OutputFolder: MergedAll
    outmergedFinal = mergeFinalparameterList(paraModelScenarioList, outFileName)
        