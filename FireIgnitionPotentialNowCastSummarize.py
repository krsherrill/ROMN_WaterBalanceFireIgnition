
# ---------------------------------------------------------------------------
# FireIgnitionPotentialNowCastSummarize.py
# Script Derive Fire Iginition Potential for short term/now cast data being pulled from a Gridmet station on Climate Analyzer.
# Fire Ignition Potential is being derived using a water balance output parameter - water deficit variable .
# Applies the Fire Ignition Potential model as defined in Thoma et. al. 2020 Management Applications Paper
# By High, Medium, and Low Fire Ignition Potential rating for Forested and Non-Forested models counts number of occurrences (i.e. days) for the previous 25 years (i.e. annual summaries).
# Additionally the Future Now Cast is summarized (High, Medium, Low) where data is available.

# Input Parameter is Cumulative Water Deficit - Water Balance parameter

#Updates:
# 20220802 - Changed the number of years to be processed from 4 to 25.
# 20230519 - Updated Fire Ignition Model to Steve Huysman Version 2.0 Southern Rockies first order https://huysman.net/research/fire/southern_rockies.html
# Fire Ignition Model vesion 2.0 included used Monitoring Trends Burn Severity data from 1984-1-1 through 2021-12-31 (Note Version FI model 1.0 used MTBS data through 2015-12-31.

#Dependicies:
#Python Version 3.9, Pandas, urllib

#Script Name: FireIgnitionPotentialNowCastSummarize.py
#Created by Kirk Sherrill - Data Manager Rock Mountian Network - I&M National Park Service
#Date: June 29, 2022

##Import Libraries
import pandas as pd, traceback, sys, os
import numpy as np
import urllib
from urllib.request import urlretrieve
import datetime
from datetime import date

#os.chdir('/var/www/html/ca_backend/python/sherrill')

###################################################
# Start of Parameters requiring set up.
###################################################

#Get Current Date
today = date.today()
strDate = today.strftime("%Y%m%d")
strCurYear = today.strftime("%Y")

#Define URL with the Gridmet data - 'strCurYear' is dynamically being used to define the current Year
serviceURL = "http://www.climateanalyzer.science/python/wb2.py?csv_output=true&station=flfograss1_from_grid&title=FLFOGRID&pet_type=hamon&max_soil_water=250&graph_table=table&table_type=daily&forgiving=very&year1=1980&year2=" + strCurYear + "&station_type=GHCN&sim_snow=true&force=true?"

inFieldFieldNowCast = 'D (MM)'  #Field in the Now Cast data being used in the fire iginition pototential model (this will be the deficit field).
siteName = 'FLFOGrass_1'  #Site Identifier - should be dynamic - not really necessary

#Define the Historic/Current reference parameters:
refYearStartDate= '1/1/1984'   #Start Year/Date for which Fire Ignition Model was evaluated (Jan 1 of Start Year)
refYearEndDate = '12/31/2021'       #End Year/Date for which Fire Ignition Model was evaluated (Dec 31 of End Year

#Output Directory/LogFile Information
outputFolder = "C:\ROMN\GIS\FLFO\LandscapeAnalysis\FireIgnition\Python\\SummarizeFLFO\\" + strDate #Folder for the output Data Package Products
#outputFolder = "./"  #Folder for the output Data Package Products

#workspace = "C:\\ROMN\\GIS\\FLFO\\LandscapeAnalysis\\FireIgnition\\NowCasts\\" + strDate + "\\workspace"   #workspace
#workspace = "./"  #workspace
workspace = outputFolder + "\\workspace"
outName = 'FireIgnitionNowCastwSummary'   #Output .csv filename
logFileName = workspace + "\\" + outName + ".LogFile.txt"
#logFileName = outName + ".LogFile.txt"

#Fire Year Parameters by Vegtation Type
nonforest_start_DOY = 79  #Non-Forested Start of Fire Year Day Number
nonforest_end_DOY = 303   #Non-Forested End of Fire Year Day Number
forest_start_DOY = 7      #Forest Start of Fire year Day Number
forest_end_DOY = 301      #Forest End of Fire year Day Number
movingWindowsDay = 7     #Number of days in the moving window average (default use 14)
#######################################
## Below are paths which are hard coded
#######################################

#################################
# Checking for directories and Log File
##################################
if os.path.exists(outputFolder):
    pass
else:
    os.makedirs(outputFolder)

if os.path.exists(workspace):
    pass
else:
    os.makedirs(workspace)

# Check if logFile exists
if os.path.exists(logFileName):
    pass
else:
    logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
    logFile.close()



def main():

    try:

        ###########################################
        #Function to pull GridMet Statation Data: Subset to 1) the NowCast Data (i.e. current date plus 60 future days) 2) All Historical data at station (e.g. 1981) through plus 60 future days
        outVal = processNowCast(serviceURL,siteName)
        if outVal[0] != "Success function":
            print("WARNING - Function processNowCast failed - Exiting Script")
            exit()
        else:

            dfPlus60 = outVal[1]

            print("Success - Function processNowCast ")

        ####################################################
        #Run Functions for the Historic/Current Time Periods
        ####################################################


        #Create Reference List for Forested Vegetation Type
        outVal = define_ReferenceList(forest_start_DOY, forest_end_DOY, refYearStartDate, refYearEndDate, dfPlus60, inFieldFieldNowCast, "DATE")
        if outVal[0] != "Success function":
            print("WARNING - Function define_ReferenceList failed - Exiting Script")
            exit()
        else:
            print("Success - Function define_ReferenceList - Forested Vegetation")
            # Assign the reference Data Frame
            refDF_Forest = outVal[1]

        # Create Reference List for Non Forest Vegetation Type
        outVal = define_ReferenceList(nonforest_start_DOY, nonforest_end_DOY, refYearStartDate, refYearEndDate, dfPlus60, inFieldFieldNowCast, "DATE")
        if outVal[0] != "Success function":
            print("WARNING - Function define_ReferenceList failed - Exiting Script")
            exit()
        else:
            print("Success - Function define_ReferenceList - Non Forest Vegetation")
            # Assign the reference Data Frame
            refDF_NonForest = outVal[1]

        # Define Moving Window Averages
        outVal = define_MovingWindowAverage(dfPlus60, movingWindowsDay, inFieldFieldNowCast)
        if outVal[0] != "Success function":
            print("WARNING - Function define_MovingWindowAverage failed - Exiting Script")
            exit()
        else:
            print("Success - Function define_MovingWindowAverages")
            # Assign output to DF
            dfHistCur_wAvg = outVal[1]


        # Define Percentiles - for Historic/Cur data, and using Forest Fire Season
        outVal = define_IgnitionProportion(dfHistCur_wAvg, inFieldFieldNowCast, refDF_Forest, inFieldFieldNowCast, "PercForest", "PropFiresForest", "Forest")
        if outVal[0] != "Success function":
            print("WARNING - Function define_IgnitionProportion failed - for 'Historic/Current - Forest Fire Season' - Exiting Script")
            exit()
        else:
            print("Success - Function define_IgnitionProportion - for 'Historic/Current - Forest Fire Season'")

            #Data Frame with the Ignition Potential Calculation
            dfwIgnition = outVal[1]
            dfwIgnition.reset_index(drop=True, inplace=True)

        #if siteType == "NonForest":

        # Define Percentiles - for Historic/Cur data, and using Grassland Fire Season
        outVal = define_IgnitionProportion(dfHistCur_wAvg, inFieldFieldNowCast, refDF_NonForest, inFieldFieldNowCast, "PercNonForest", "PropFiresNonForest", "NonForest")
        if outVal[0] != "Success function":
            print("WARNING - Function define_IgnitionProportion failed - for 'Historic/Current - NonForest Fire Season' - Exiting Script")
            exit()
        else:
            print("Success - Function define_IgnitionProportion - for 'Historic/Current - NonForest Fire Season'")

            #Data Frame with the Ignition Potential Calculation
            dfwIgnitionGrass = outVal[1]
            dfwIgnitionGrass.reset_index(drop=True, inplace=True)


        ############################################
        # Create Datasets Historic last four years
        ############################################
        # Define Last four full years
        today = date.today()
        strYearNow = int(today.strftime("%Y"))
        #startYear = strYearNow - 4
        startYear = strYearNow - 25   #Changed from 4 year to evaluating the last 25 years.
        endYear = strYearNow - 1
        # Create List of Years to be processed
        rangeList = [*range(startYear, endYear+1)]
        #List to hold the processed dataframes
        appendDfList = []
        #Loop For NonForest
        for year in rangeList:
            #Run for Singular Years Start Year Thru End Year
            outFun = summarizeFireDangerRating(dfwIgnitionGrass, 'SiteName', 'FLFO', 'PercNonForest', year, year, 'Non-Forest', 'Mean', 'Year', 'na', 'DATE', 'no')
            if outFun[0] != "Success Function":
                print("WARNING - Function summarizeFireDangerRating failed - Exiting Script")
                exit()

            else:
                print("Success - Function summarizeFireDangerRating - Forested Vegetation - " + str(year) + " - Veg Type - NonForest")
                # Define the output dataframe
                appendDfList.append(outFun[1])

        #Loop for Forest
        for year in rangeList:
            #Run for Singular Years Start Year Thru End Year
            outFun = summarizeFireDangerRating(dfwIgnition, 'SiteName', 'FLFO', 'PercForest', year, year, 'Forest', 'Mean', 'Year', 'na', 'DATE', 'no')
            if outFun[0] != "Success Function":
                print("WARNING - Function summarizeFireDangerRating failed - Exiting Script")
                exit()

            else:
                print("Success - Function summarizeFireDangerRating - Forested Vegetation - " + str(year) + " - Veg Type - NonForest")
                # Define the output dataframe
                appendDfList.append(outFun[1])



        ############################################
        #Process the Plus 60 Now Cast Only Dataframe to only Now Cast Records with a deficit value
        ############################################
        dfwIgnition.to_csv(workspace + "\\dfwIgnition.csv")

        'Now Cast Forest'
        outFun = subsetToNowCast(dfwIgnition)
        if outFun[0] != "Success Function":
            print("WARNING - Function 'subsetToNowCast' Forest failed - Exiting Script")
            exit()

        else:
            dfwIgnitionForestNowCast = outFun[1]
            print("Success - Function 'subsetToNowCast' Forest")

        #Summarize the nowcast data - Forest
        # Run for Singular Years Start Year Thru End Year
        outFun = summarizeFireDangerRating(dfwIgnitionForestNowCast, 'SiteName', 'FLFO', 'PercForest', 'NowCast', 'NowCast', 'Forest','Mean', 'Year', 'na', 'DATE', 'yes')
        if outFun[0] != "Success Function":
            print("WARNING - Function summarizeFireDangerRating failed Forested Vegetation Now Cast- Exiting Script")
            exit()

        else:
            print("Success - Function summarizeFireDangerRating - Forested Vegetation Now Cast")
            # Define the output dataframe
            appendDfList.append(outFun[1])

        'Now Cast NonForest'
        outFun = subsetToNowCast(dfwIgnitionGrass)
        if outFun[0] != "Success Function":
            print("WARNING - Function 'subsetToNowCast' Non Forest failed - Exiting Script")
            exit()

        else:
            dfwIgnitionNonForestNowCast = outFun[1]
            print("Success - Function 'subsetToNowCast' Non Forest")

        # Summarize the nowcast data - Forest
        # Run for Singular Years Start Year Thru End Year
        outFun = summarizeFireDangerRating(dfwIgnitionNonForestNowCast, 'SiteName', 'FLFO', 'PercNonForest', 'NowCast', 'NowCast', 'Non-Forest', 'Mean', 'Year', 'na', 'DATE', 'yes')
        if outFun[0] != "Success Function":
            print("WARNING - Function summarizeFireDangerRating failed Non Forest Vegetation Now Cast- Exiting Script")
            exit()

        else:
            print("Success - Function summarizeFireDangerRating - Non Forested Vegetation Now Cast")
            # Define the output dataframe
            appendDfList.append(outFun[1])


        ################################
        # Append all processed Time Frames
        ################################
        outFun = appendFiles(appendDfList)
        if outFun[0].lower() != "success function":
            print("WARNING - Function appendFiles failed - Exiting Script")

        else:
            # Push non-ensemble output to a data frame
            dfallFiles = outFun[1]

            messageTime = timeFun()
            scriptMsg = "Success - Processing appended last Four Year Summaries - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")


        # Export output final dataframe
        currentDate = datetime.date.today()
        strCurrentDate = str(currentDate)
        outFull = outputFolder + "\\" + outName + "_" + strCurrentDate + ".csv"
        #outFull = outName + ".csv"

        # Export
        dfallFiles.to_csv(outFull, index=False)


        scriptMsg = "Successfully processed Now Cast Data - Single Years and Now Casst: " + outFull + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")


    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - FireIgnitionPotentialScript - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

#Get count of Now Cast with Data
def numberNowCastDays3(inDf):

    try:
        # Change Deficit to Numeric - coerce so no data is NaN
        inDf['D (MM)'] = pd.to_numeric(inDf['D (MM)'], errors='coerce')

        # Get Count of records with deficit value
        futureRecordCount = inDf['D (MM)'].count()

        return "Success Function", futureRecordCount
    except:
        messageTime = timeFun()
        print("Error on numberNowCastDays3 Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'numberNowCastDays3'"



# Routine to get the summary statistic Value for the defined time period for the Fire Iginition Potential Fire Danger Rating
# Input: inFile - table full path with the Fire Ignition Potential Data to be summarizes
# inFileFieldAOA - Field in 'inFile that defines the site/AOA to be summarized
# inAOAWildcard - Syntax used to filter the 'inFileFieldAOA' records to be processed
# inFieldPerc - 'Field in 'inFile' with the derived 'Fire Ignition Potential' percentile rating (i.e. the Fire Ignition Potential Field to be summarized)
# startYear - first year to be processed
# endYear - end year to be processed
# coverType - defined the fire ignition potential relationship type to be applied ('Forest|'Non-Forest') - defines the Percentile values to be used for High, Medium,
# Low fire Danger Ratings

def summarizeFireDangerRating(inDf, inFileFieldAOA, inAOAWildcard, inFieldPerc, startYear, endYear, coverType, statistic, timeStep, rcp, timeField, NowCast):
    try:

        if NowCast != "yes":  #Processing Yearly or Normal
            # Subset the DF to the desired Reocrds (i.e. Forest or Non-Forest Sites)
            df2 = inDf[inDf[inFileFieldAOA].str.contains(inAOAWildcard, na=False)]
            # Convert the time variable to a datetime type
            df2['time'] = pd.to_datetime(df2[timeField], utc='None', errors='coerce')

            # Summary the Fire Danger Ratings, High, Medium, Low for the defined records, across the defined temporary time frame
            # 1) Subset to the defined year range
            # Convert the start and end year values to a datetime variable

            # Convert the integer startYear and endYear inputs to a DateTime variable
            startYearDT = date(year=startYear, month=1, day=1)
            endYearDT = date(year=endYear, month=12, day=31)

            # Create initial StartDate, EndDate fieds not UTC aware
            df2['StartDate'] = startYearDT
            df2['EndDate'] = endYearDT

            # Define StartDate and EndDate fields in dataframe as DateTime UTC aware
            df2['StartDate'] = pd.to_datetime(df2['StartDate'], utc='None', errors='coerce')
            df2['EndDate'] = pd.to_datetime(df2['EndDate'], utc='None', errors='coerce')

            # Subset a second  time to the start and end date records
            df3 = df2.loc[(df2['time'] >= df2['StartDate']) & (df2['time'] <= df2['EndDate'])]

        else: #processing NowCast dataframe

            df3= inDf


        # 2)Apply the Ratings creating three fields High, Medium, Low - Binary
        # Define the Fire Danger Classes
        outFun = fireDangerCategories(coverType)
        highFireVal = outFun[0]
        mediumFireVal = outFun[1]

        # Apply the Binary Fire Ratings High, Medium, Low as new fields evaluated against the 'inFieldPerc' percentile field
        df3['High'] = np.where((df3[inFieldPerc] > highFireVal), 1, 0)
        df3['Medium'] = np.where((df3[inFieldPerc] <= highFireVal) & (df3[inFieldPerc] > mediumFireVal), 1, 0)
        df3['Low'] = np.where((df3[inFieldPerc] <= mediumFireVal), 1, 0)

        # 3)resample to Yearly with .sum
        # Set the Row Index value to 'Time' to allow for Time Series calculation, must set inplace=True so copy of 'DateTime
        df3.set_index(df3[timeField], inplace=True)


        # Resample and Summarize High
        outFun = resampleSummarize(df3, 'High', 'Annual', 'Sum')
        if outFun[0] != "Success Function":
            print("WARNING - Function resampleSummarize failed for Summarize High - Exiting Script")
        else:
            outDataFrame = outFun[1]

        # Resample and Summarize Medium
        outFun = resampleSummarize(df3, 'Medium', 'Annual', 'Sum')
        if outFun[0] != "Success Function":
            print("WARNING - Function resampleSummarize failed for Summarize Medium - Exiting Script")
        else:

            outDataFrameCur = outFun[1]
            # Rename Columns prior to join so can be dropped
            outDataFrameCur.rename(columns={timeField: "timeDrop"}, inplace=True)

            # Concatenate (i.e. join) Output Dataframe High and Medium
            dfHighMed = pd.concat([outDataFrame, outDataFrameCur], axis=1, join='inner')

            # Drop duplicate 'DateTime' field
            dfHighMed.drop(['timeDrop'], axis=1, inplace=True)

        # Resample and Summarize Low
        outFun = resampleSummarize(df3, 'Low', 'Annual', 'Sum')
        if outFun[0] != "Success Function":
            print("WARNING - Function resampleSummarize failed for Summarize Medium - Exiting Script")
        else:
            outDataFrameCur = outFun[1]
            # Rename Columns prior to join  so can be dropped
            outDataFrameCur.rename(columns={timeField: "timeDrop"}, inplace=True)

            # Concatenate (i.e. join) Output Dataframe High and Medium
            dfHighMedLow = pd.concat([dfHighMed, outDataFrameCur], axis=1, join='inner')

            # Drop duplicate 'DateTime' field
            dfHighMedLow.drop(['timeDrop'], axis=1, inplace=True)

            # Rename column time to DateTime
            dfHighMedLow.rename(columns={timeField: "DateTime"}, inplace=True)
            # Define the SiteName Field in output dataframe
            siteNameLU = df3['SiteName'].values[0]
            dfHighMedLow.insert(loc=0, column='SiteName', value=siteNameLU)
            # Add CoverType Field
            dfHighMedLow.insert(loc=2, column='CoverType', value=coverType)

        # 4) Calculate the MEan Normal from the previously derived annual sums
        if statistic == 'Mean' and timeStep == 'Normal' or timeStep == 'Year':
            dfNormals = dfHighMedLow.groupby([inFileFieldAOA,'CoverType']).mean()

            # Add an RCP field
            dfNormals.insert(loc=0, column='RCP', value=rcp)

            #Add GCM field
            if rcp == 'na':
                gcm = 'na'
            else:
                split = inFieldPerc.split("_")
                gcm = split[1]

            dfNormals.insert(loc=0, column='GCM', value=gcm)

            #Define the output DateTime field
            #outDataTimeDef = statistic + "_" + timeStep + "_" + str(startYear) + "_" + str(endYear)
            outDataTimeDef = str(startYear) + "_" + str(endYear)

            #Add the DateTime Field
            dfNormals.insert(loc=0, column='DateTime', value=outDataTimeDef)

            # Add Cover Type
            dfNormals.insert(loc=0, column='CoverType', value=coverType)

            #Add Site Name Field
            dfNormals.insert(loc=0, column='SiteName', value=siteNameLU)

            #Rename High_Sum, Medium_Sum, and Low_Sum fields to High_Mean, Medium_Mean, Low_Mean
            dfNormals.rename(columns={"High_Sum": "High_Mean", "Medium_Sum": "Medium_Mean", "Low_Sum": "Low_Mean"}, inplace=True)

        else:
            print ("Undefined 'statistic' - " + statistic + " - exiting script with error")
            sys.exit ("Undefined 'statistic' - " + statistic + " - exiting script with error")


        if NowCast.lower() == "yes": #Add the Now Cast Count Field
            ########################################
            # Get number of nowcast days with records.
            outFun = numberNowCastDays3(df3)
            if outFun[0].lower() != "success function":
                print("WARNING - Function 'numberNowCastDays3' failed - Exiting Script")

            else:
                # Push NowCast Df to the
                numberNowCast = outFun[1]

                messageTime = timeFun()
                print("Success - numberNowCastDays3 - " + messageTime)

            rows = dfNormals.shape[1]
            dfNormals.insert(loc=rows, column='NowCastCount', value=numberNowCast)

        return "Success Function", dfNormals
    except:
        messageTime = timeFun()
        print("Error on summarizeFireDangerRating Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'summarizeFireDangerRating'"


# Append Files in list to .csv file
def appendFiles(appendList):
    try:

        for count, file in enumerate(appendList):

            dfLoop = appendList[count]
            if count == 0:  # Make new dfallFiles

                dfallFiles = dfLoop

            # Append dfLoop to dfallFiles
            else:

                dfallFiles = dfallFiles.append(dfLoop, ignore_index=True, verify_integrity=True)

            del dfLoop

        return "Success function", dfallFiles

    except:
        messageTime = timeFun()
        print("Error on appendFiles Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'appendFiles'"

# Function summarizes the input DataFrame, defined field, for the desired timestep (e.g. Yearly), and defined summary statistic
# output is a DataFrame with the Rasample Summary Statistic
def resampleSummarize(inDataFrame, processField, timeStep, sumStat):
    try:

        # Calculate the Resample and Summary Statistic
        if sumStat == 'Sum' and timeStep == 'Annual':

            outSumSeries = inDataFrame[processField].resample(rule='AS').sum()


        else:
            print("'Time Step' variable - " + timeStep + " - or Summary Statistic - " + sumStat + " combo is not defined' - existing script Failed")
            sys.exit("'Time Step' variable - " + timeStep + " - or Summary Statistic - " + sumStat + " combo is not defined' - existing script Failed")

        outResampleDf = outSumSeries.to_frame().reset_index()

        # Define the derived Output Summary field name
        outFieldName = processField + "_" + sumStat
        # Export the Output Summary Series to a DataFrame
        outResampleDf.rename(columns={processField: outFieldName}, inplace=True)

        return "Success Function", outResampleDf

    except:


        print("Error on resampleSummarize Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'resampleSummarize'"


# Function defines the fire danger percentile breaks for High, Medium and Low by Cover Type Forest or Non-Forest.  See Figure 6 Thoma et. al. 2020
def fireDangerCategories(coverType):
    try:

        if coverType == 'Forest':
            # Thoma et. al. 2020 Fire Ignition Model Version 1.0 Southern Rockies Equation.
            # highFire = 86
            # mediumFire = 65

            # Steve Huysman 2023 Fire Ignition Model Version 2.0 Fire Order Model for the Southern Rockies.
            highFire = 84
            mediumFire = 53


        elif coverType == 'Non-Forest':
            # Thoma et. al. 2020 Fire Ignition Model Version 1.0 Southern Rockies Equation.
            # highFire = 90
            # mediumFire = 73

            # Steve Huysman 2023 Fire Ignition Model Version 2.0 Fire Order Model for the Southern Rockies.
            highFire = 84
            mediumFire = 51

        else:
            print("'coverType' variable is not defined as 'Forest' or 'Non-Forest' - existing script Failed")
            sys.exit("'coverType' variable is not defined as 'Forest' or 'Non-Forest' - existing script Failed")

        return highFire, mediumFire

    except:
        messageTime = timeFun()
        print("Error on fireDangerCategories Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'fireDangerCategories'"

#Functions Below
# Function to Get the Date/Time
def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime

# Function Creates a new DataFrame from the defined Columns/Series in a pre-existing dataframe
def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame


#Function - Subsets the Climate Analyzer Rest data to two data Frames:
# Outputs:
# nowCastDfSubset - Datframe with only data current date plus 60 days to be used for now cast summarization
# dfPlus60 - Dataframe from 1981 through todays date plus 60 days for the now cast
def processNowCast(serviceURL,siteName):
    try:

        # Import the data from climate analyzer
        workCSVFile = workspace + "\\gridmetData_wWB.csv"
        # Export Rest pull to .csv
        urlretrieve(serviceURL, workCSVFile)
        # Import work CSV file to dataframe  - Skipping first 5 head rows, and skipping footer rows - this should be dynamic
        # stop import at the first 'Text Line?'
        nowCastDf = pd.read_csv(workCSVFile, skiprows=5, engine='python')

        # Clean-up Dataframe
        # Trim white space from Field Names:
        nowCastDf.columns = nowCastDf.columns.str.strip()
        # Delete no data nan column being created on strip
        nowCastDf.drop([""], axis=1, inplace=True)
        # Trim Leading White Space from DATE
        nowCastDf['DATE'] = nowCastDf['DATE'].str.strip()

        #Define Current Date plus 60
        currentDate = datetime.date.today()
        # Add 60 days to current datatime
        currentDatePlus60 = currentDate + datetime.timedelta(days=60)
        # Define as String
        stringFormat = currentDatePlus60.strftime('%m/%d/%Y')

        # Find Record with the plus 60 date
        indexDf = nowCastDf.loc[nowCastDf['DATE'] == stringFormat]
        indexSubSet = indexDf['INDEX'].astype(int)

        # Get the Index Value
        indexSubSet = indexSubSet.index
        # Extract Index Value and add 1 - will be used to subset
        indexValuePlus1 = indexSubSet[0] + 1

        # Subset Records to plus 60 only removing the monthly summaries Mike included in the rest pull - this will be retained for historical calculations
        dfPlus60 = nowCastDf[0:indexValuePlus1]

        #Convert 'DATE' field to Date Time now that text values have been removed
        dfPlus60['DATE'] = pd.to_datetime(dfPlus60['DATE'], infer_datetime_format=True)

        # currentDate = pd.to_datetime(currentDate, infer_datetime_format=True)
        # currentDatePlus60 = pd.to_datetime(currentDatePlus60, infer_datetime_format=True)
        # #Subset to the Current Date Plus 60 records
        # nowCastDfSubset = dfPlus60[(dfPlus60['DATE'] >= currentDate) & (dfPlus60['DATE'] <= currentDatePlus60)]

        del(nowCastDf)
        # Add SiteNameField
        dfPlus60.insert(1, "SiteName", siteName, True)


        return "Success function", dfPlus60
    except:

        messageTime = timeFun()
        print("Error on define_ReferenceList Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'define_ReferenceList'"



###############################################################################################################
#Function 1. Routine to Derive the Historical 1984-2015 Fire Period Water Deficit full list for the Fire Period
###############################################################################################################
# startDate - First date of fire period across the 1984-2015 fire period
# enddate - Last date of fire period across the 1984-2015 fire period
# stationData - station data being processed
# inFileField - Parameter being evaluated (e.g. Deficit)
# inFileTime - Parameter defining the time field

# Output - pandas data series (i.e. 1D) with the input variable (e.g. Deficit) trimmed to the defined fire year range
def define_ReferenceList(startDate, endDate, refYearStartDate, refYearEndDate, stationData, inFileField, inFileTime):
    try:

        'Make data frame from Input List'
        #df = pd.read_csv(stationData)
        df = stationData
        selected_columns = []
        selected_columns.append(inFileTime)
        selected_columns.append(inFileField)

        # Function to take the defined Columns and Export the Columns to a new Pandas Data Frame
        df2 = select_columns(df, selected_columns)  # Creating new data frome with only the date and time series field being processed

        # Create 'timeDate' field as datetime filed
        df2["timeDate"] = pd.to_datetime(df2[inFileTime])
        # Create the Day of Year Field
        df2["DOY"] = df2['timeDate'].dt.dayofyear

        #Select where Day of Year >=Start Date and <= End Date
        outDfFireYear1 = df2[(df2['DOY'] >= startDate) & (df2['DOY'] < endDate)]

        #Select Second subset fire years of interest Year >= refYearStart Date and <= refYearEnd
        #Convert Inputs to DateTime parameters
        refYearStartDT = pd.to_datetime(refYearStartDate, format='%m/%d/%Y')
        refYearEndDT = pd.to_datetime(refYearEndDate, format='%m/%d/%Y')

        outDfFireYear = outDfFireYear1[(outDfFireYear1['timeDate'] >= refYearStartDT) & (outDfFireYear1['timeDate'] <= refYearEndDT)]


        #Reset Index to 0
        outDfFireYear.reset_index(drop=True, inplace = True)

        del outDfFireYear1
        return "Success function", outDfFireYear
    except:

        messageTime = timeFun()
        print("Error on define_ReferenceList Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'define_ReferenceList'"


#####################################################################
#Function 2. Define the 14 Day Moving Average Values by input Dataset
#####################################################################
# Input:
# - inDataSet: Dataset which is being evaluated (e.g. Historical/Current/NowCast/Future Projects)
# - movingWindowDays: Moving window number of days prior to the date to be derived (Default will be 14 day prior)
# - fieldToAverage: Field in the 'inDateSet' being averaged
# Output - New Field -'MovingAverage_{movingWindowDays}' in output dataframe with the moving window Average

# Note in Rolling/Moving window average min_periods is set to 1 (i.e. only 1 day of data needed)
def define_MovingWindowAverage(inDataSet, movingWindowDays, fieldToAverage):
    try:

        # Make data frame from Input
        #df = pd.read_csv(inDataSet)

        outField = fieldToAverage + "_" + str(movingWindowDays)
        # Use Rolling in dataframe to calculate the Moving Window Average
        #df[outField] = df[fieldToAverage].rolling(movingWindowDays, min_periods=1, win_type=None).mean()
        inDataSet[outField] = inDataSet[fieldToAverage].rolling(movingWindowDays, min_periods=1, win_type=None).mean()

        #Reset Index to Zero
        inDataSet.reset_index(drop=True, inplace = True)

        return "Success function", inDataSet
    except:

        messageTime = timeFun()
        print("Error on define_MovingWindowAverage Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'define_MovingWindowAverage'"

#####################################################################
#Function 3. Routine to Derive the Fire Ignition Potential Proportion
#Referenced against the trimmed dataframe (i.e. Forest and Non Forest Fire Season 1984-2015
# Applying the Fire Ignition Potential Relationship in Thoma et. al. 2020 Global Ecology Conservation
#####################################################################
#Non-Forest Equation where percentile is the percentile and f is the percent of historic fires (1984-2015) middle rockies
#that has ignited under the percentile dryness conditions
def nonforest_equation(percentile):
    # f = 0.047 * np.e**(0.075*percentile)  #Thoma et. al. 2020 Fire Ignition Model Version 1.0 Southern Rockies Equation.
    f = 0.0119265 * np.e ** (4.192916 * percentile)  # Steve Huysman 2023 Fire Ignition Model Version 2.0 Fire Order Model for the Southern Rockies.
    return f

#Forest Equation where percentile is the percentile and f is the percent of historic fires (1984-2015) middle rockies
#that has ignited under the percentile dryness conditions
def forest_equation(percentile):
    # f = 0.368 * np.e**(0.055*percentile)  #Thoma et. al. 2020 Fire Ignition Model Version 1.0 Southern Rockies Equation.
    f = 0.0095308 * np.e ** (4.4556479 * percentile)  # Steve Huysman 2023 Fire Ignition Model Version 2.0 Fire Order Model for the Southern Rockies.
    return f


#Subset to only NowCast Data - passing data frames that already have fire ignition potential calculated
def subsetToNowCast(inDf):

    try:

        # Define Current Date plus 60
        currentDate = datetime.date.today()
        # Add 60 days to current datatime
        currentDatePlus60 = currentDate + datetime.timedelta(days=60)
        # Define as String
        stringFormat = currentDatePlus60.strftime('%Y-%m-%d')

        # Find Record with the plus 60 date
        indexDf = inDf.loc[inDf['DATE'] == stringFormat]
        indexSubSet = indexDf['INDEX'].astype(int)

        # Get the Index Value
        # Get the Index Value
        indexSubSet = indexSubSet.index
        # Extract Index Value and add 1 - will be used to subset - not being used
        indexValuePlus1 = indexSubSet[0] + 1

        # Convert 'DATE' field to Date Time now that text values have been removed
        inDf['DATE'] = pd.to_datetime(inDf['DATE'], infer_datetime_format=True)

        currentDate = pd.to_datetime(currentDate, infer_datetime_format=True)
        currentDatePlus60 = pd.to_datetime(currentDatePlus60, infer_datetime_format=True)

        # subset dataframe to >= currentDateTime and <= curerntDateTimePlus60
        nowCastDfSubset = inDf[(inDf['DATE'] >= currentDate) & (inDf['DATE'] <= currentDatePlus60)]

        # Force Deficit Field to Numeric makeing all non Numeric a Pandas 'NAN'
        nowCastDfSubset['D (MM)'] = pd.to_numeric(nowCastDfSubset['D (MM)'], errors='coerce')

        #Retain only records with Data
        nowCastDfSubset['IsNull'] = nowCastDfSubset['D (MM)'].isnull()

        nowCastDfSubset2 = nowCastDfSubset.loc[(nowCastDfSubset['IsNull'] == False)]

        #Delete 'IsNull' field
        nowCastDfSubset2.drop(['IsNull'], axis=1, inplace=True)

        del nowCastDfSubset
        return "Success Function", nowCastDfSubset2

    except:

        messageTime = timeFun()
        print("Error on subsetToNowCast Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'subsetToNowCast'"


# Input:
# 1. processDF - Data frame being processed with 14 Day Moving Averages
# 2. field14Average - Field in 'processDF' with the 14 Day Moving Averages
# 3. refDF - Reference DataFrame that has been trimmed to the fire year only values
# 4. refDFField - Field in 'refDF' being checked (e.g. 'inFileField' or 'inFileFieldProj')
# 5. percentileField - output Percentile field in the 'processDF' (e.g. 'PercentileForest','PercentileNonForest')
# 6. ignitionProportionField - output Fire Ignition Proportion field in the 'processDF' (i.e. the Proportion of Fires for either Forest or Non-Forest)
# 7. equationType - defines if Forest or Non-Forest Equations (i.e. function - 'forest_equation' or 'Non-Forest' will be used to
#               derive the Fire Ignition Proportion Value

# Fire Iginition Proportion Equation is derived from Thoma et. al. 2020 Global Ecology Conservation.
# Output - 'processDF' Data Frame with percentile fields and Fire Ignition Proportion field

def define_IgnitionProportion(processDF, field14Average, refDF, refDFField, percentileField, ignitionPropField, equationType):
    try:

        # Field with Averages
        processField = field14Average + "_" + str(movingWindowsDay)

        # Add Percentile Field
        lastColumn = processDF.shape[1]
        processDF.insert(lastColumn, percentileField, "")
        # Change 'PercentileField' Data Type to Float
        processDF[percentileField] = pd.to_numeric(processDF[percentileField], errors='coerce')

        # Add 'ignitionPropField' (i.e. Ignition Potential Field)
        lastColumn = processDF.shape[1]
        processDF.insert(lastColumn, ignitionPropField, "")
        # Change 'PercentileField' Data Type to Float
        processDF[ignitionPropField] = pd.to_numeric(processDF[ignitionPropField], errors='coerce')

        #Converting the reference dataframe deficit field to numeric
        refDF[refDFField] = pd.to_numeric(refDF[refDFField], errors='coerce')

        # Number of records in the reference dataset - (i.e denominator) - Only interesting if count for the Respective Fire Season
        # By Type Forest or Grassland(i.e. 1984-2015)
        totalRecordsFireSeason = refDF.shape[0]

        # Records to iterate through in processDF
        totalRecordsProcess = processDF.shape[0]

        rowRange = range(0, totalRecordsProcess)
        # Loop thru the processDF dataframe and assign a percentile per record
        for row in rowRange:

            rowSeries = processDF.iloc[row]
            checkValue = rowSeries.get(processField)

            # Get Count of records in the 'refDF' (i.e. Reference DF) and Field that are greater than the input value in the 'processDF'
            n = np.sum(refDF[refDFField] < checkValue)

            # Calculate Percentile
            percentile = (n / totalRecordsFireSeason) * 100

            # Assign the percentile value to the processDf and 'percentileField'
            processDF.at[row, percentileField] = percentile

            # Calculation the Proportion of Fire Ignition
            if equationType == "Forest":
                outIgnitionPerc = forest_equation(percentile)

            if equationType == "NonForest":
                outIgnitionPerc = nonforest_equation(percentile)

            # Assign IgnitionPercentage to the 'processDF' dataframe
            processDF.at[row, ignitionPropField] = outIgnitionPerc

        return "Success function", processDF
    except:

        messageTime = timeFun()
        print("Error on define_IgnitionProportion Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'define_IgnitionProportion'"


if __name__ == '__main__':

    # Analyses routine ---------------------------------------------------------
    main()
