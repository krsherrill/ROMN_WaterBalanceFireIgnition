
# ---------------------------------------------------------------------------
# FireIgnition_SummarizeNormals.py
#Script Summarizes The Fire Iginiton Potential Data by AOA Field, Site Wild Card (only singular processing by defined row in inProcessFile)
#field to be summarized (i.e. respective percentiles outputs (e.g. by GCP, Forest, Non-Forest, etc.), start and end Years,
#Currently defined to run a 'Mean' normal across the defined years.

#Additonally the script will calculate ensemble averages across the General Circulatoin Model, RCP, and defined time frames (e.g. 2031-2060 and 2061-2090).

#Dependicies:
#Python Version 3.x, Pandas, numpy

#Script Name: FireIgnition_SummarizeNormals.py
#Notebook created by Kirk Sherrill - Data Manager Rock Mountian Network - I&M National Park Service
#Date - October 18th, 2021

#Import Libraries
import pandas as pd, traceback, sys, os
import numpy as np
from datetime import datetime
from datetime import date



#Get Current Date
today = date.today()
strDate = today.strftime("%Y%m%d")


###################################################
# Start of Parameters requiring set up.
###################################################
#Excel file defining the Fire Ignition records to be processed
inProcessFile = r'C:\ROMN\GIS\FLFO\LandscapeAnalysis\FireIgnition\Python\SummarizeFLFO\20220922\HistoricCurrentProcessingList_20220922.xlsx'
#Output Directory/LogFile Information
outDirectory = "C:\ROMN\GIS\FLFO\LandscapeAnalysis\FireIgnition\Python\SummarizeFLFO\\" + strDate #Folder for the output Data Package Products

workspace = outDirectory + "\\workspace"   #workspace
outFileName = 'FLFO_FireDangerSummary_HistCurrentFutures_Normals' + strDate   #Output .csv filename
logFileName = workspace + "\\" + outFileName + ".LogFile.txt"

#######################################
## Below are paths which are hard coded
#######################################
#
#################################
# Checking for working directories and Log File
##################################
if os.path.exists(workspace):
    pass
else:
    os.makedirs(workspace)

if os.path.exists(outDirectory):
    pass
else:
    os.makedirs(outDirectory)

# Check if logFile exists
if os.path.exists(logFileName):
    pass
else:
    logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
    logFile.close()

#################################################
##
def main():

    try:

        processDf = pd.read_excel(inProcessFile)
        shapeOutput = processDf.shape
        rowCount = (shapeOutput[0])
        rowRange = range(0, rowCount)

        #Define List to hold the output DF which will be appended to one dataframe and exported
        appendDfList = []

        for row in rowRange:

            # Push Row to a panadas series
            rowValues = processDf.iloc[row]
            inFile = rowValues[0]
            inFileFieldAOA = rowValues[1]
            inAOAWildcard = rowValues[2]
            inFieldPerc = rowValues[3]
            startYear = rowValues[4]
            endYear = rowValues[5]
            coverType = rowValues[6]
            statistic = rowValues[7]
            timeStep = rowValues[8]
            rcp = rowValues[9]
            #timeField = rowValues.get(['timeField'])
            timeField = rowValues[10]

            outFun = summarizeFireDangerRating (inFile, inFileFieldAOA, inAOAWildcard, inFieldPerc, startYear, endYear, coverType, statistic, timeStep, rcp, timeField)
            if outFun[0] !=  "Success Function":
                print("WARNING - Function summarizeFireDangerRating failed - Exiting Script")
                exit()

            else:
                print("Success - Function summarizeFireDangerRating")
                # Define the output dataframe
                appendDfList.append(outFun[1])

            messageTime = timeFun()
            scriptMsg = "Successfully processed row: " + str(row) + " - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")


        ################################
        #Append all processed Time Steps Non-Ensemble
        ################################
        outFun = appendFiles(appendDfList)
        if outFun[0] != "Success function":
            print("WARNING - Function summarizeFireDangerRating failed - Exiting Script")

        else:
            #Push non-ensemble output to a data frame
            dfallFiles = outFun[1]

            messageTime = timeFun()
            scriptMsg = "Success - Processing Non-Ensemble Averages - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")

        ################################################################
        # Calculate the Ensemble Averages for All RCP 4.5 and RCP 8.5 GCM
        #Define second append list
        appendDfList2 = []
        #Add Non-ensemble DF to append list
        appendDfList2.append(dfallFiles)

        df = dfallFiles

        # Calculate Forest RCP 45 2031-2060
        siteNameLU = 'FLFOForest_1'
        CoverTypeLU = 'Forest'
        RCPLU = 'rcp45'
        DateTimeLU = '2031_2060'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]


        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:

            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])


        # Calculate Non-Forest RCP 45 2031-2060
        siteNameLU = 'FLFOGrass_1'
        CoverTypeLU = 'Non-Forest'
        RCPLU = 'rcp45'
        DateTimeLU = '2031_2060'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:

            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        # Calculate Forest RCP 85 2031-2060
        siteNameLU = 'FLFOForest_1'
        CoverTypeLU = 'Forest'
        RCPLU = 'rcp85'
        DateTimeLU = '2031_2060'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:
            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        # Calculate Non-Forest RCP 85 2031-2060
        siteNameLU = 'FLFOGrass_1'
        CoverTypeLU = 'Non-Forest'
        RCPLU = 'rcp85'
        DateTimeLU = '2031_2060'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:
            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        # Calculate Forest RCP 45 2061-2090
        siteNameLU = 'FLFOForest_1'
        CoverTypeLU = 'Forest'
        RCPLU = 'rcp45'
        DateTimeLU = '2061_2090'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:
            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        # Calculate Non-Forest RCP 45 2061-2090
        siteNameLU = 'FLFOGrass_1'
        CoverTypeLU = 'Non-Forest'
        RCPLU = 'rcp45'
        DateTimeLU = '2061_2090'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:

            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        # Calculate Forest RCP 85 2061_2090
        siteNameLU = 'FLFOForest_1'
        CoverTypeLU = 'Forest'
        RCPLU = 'rcp85'
        DateTimeLU = '2061_2090'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:
            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        # Calculate Non-Forest RCP 85 2061_2090
        siteNameLU = 'FLFOGrass_1'
        CoverTypeLU = 'Non-Forest'
        RCPLU = 'rcp85'
        DateTimeLU = '2061_2090'
        dfSubset = df[(df["CoverType"] == CoverTypeLU) & (df["RCP"] == RCPLU) & (df["DateTime"] == DateTimeLU)]

        outFun = calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU)
        if outFun[0] != "Success Function":
            print("WARNING - Function calculateEnsembleAvg failed - Exiting Script")
            sys.exit()
        else:
            messageTime = timeFun()
            scriptMsg = "Success Function: calculateEnsembleAvg - " + siteNameLU + " - " + CoverTypeLU + " - " + RCPLU + " - " + DateTimeLU + " - " + messageTime
            print(scriptMsg)
            appendDfList2.append(outFun[1])

        ################################
        # Append all processed Time Steps
        ################################
        outFun = appendFiles(appendDfList2)
        if outFun[0] != "Success function":
            print("WARNING - Function summarizeFireDangerRating failed - Exiting Script")

        else:

            dfallFiles2 = outFun[1]

            messageTime = timeFun()
            scriptMsg = "Success - Processing - FireIgnition_SummarizeScript.py - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")


            #Export Final DataFrame
            # Define Export .csv file
            outFull = outDirectory + "\\" + outFileName + ".csv"

            # Export
            dfallFiles2.to_csv(outFull, index=False)



    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - FireIgnitionPotentialScript - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

#Functions Below
# Function to Get the Date/Time
def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime


#Calculate the ensemble average for passed dataFrame and creates dataFrame to be appended
#Will derive the average value for High, Medium, and Low fields
#Inputs:  SiteNameLU - siteName being passed
#coverTypeLU - cover Type variable being passed
#RCPLU - RPC variable being passed
#DateTimeLU - Date Time being passed

def calculateEnsembleAvg(dfSubset, siteNameLU, CoverTypeLU, RCPLU, DateTimeLU):
    try:

        #Create any empty data frame to which the ensemble values will be defined
        dfEnsemble = pd.DataFrame(data=None, columns=dfSubset.columns)


        #Define Records Fields
        dfEnsemble.at[0, 'SiteName'] = siteNameLU
        dfEnsemble.at[0, 'CoverType'] = CoverTypeLU
        dfEnsemble.at[0, 'DateTime'] = DateTimeLU
        dfEnsemble.at[0, 'GCM'] = 'Ensemble'
        dfEnsemble.at[0, 'RCP'] = RCPLU


        # High
        outStat = dfSubset['High_Mean'].mean()
        dfEnsemble.at[0, 'High_Mean'] = outStat

        # Medium
        outStat = dfSubset['Medium_Mean'].mean()
        dfEnsemble.at[0, 'Medium_Mean'] = outStat

        # Low
        outStat = dfSubset['Low_Mean'].mean()
        dfEnsemble.at[0, 'Low_Mean'] = outStat

        return "Success Function", dfEnsemble

    except:


        print("Error on resampleSummarize Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'calculateEnsembleAvg'"



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


# Routine to get the summary statistic Value for the defined time period for the Fire Iginition Potential Fire Danger Rating
# Input: inFile - table full path with the Fire Ignition Potential Data to be summarizes
# inFileFieldAOA - Field in 'inFile that defines the site/AOA to be summarized
# inAOAWildcard - Syntax used to filter the 'inFileFieldAOA' records to be processed
# inFieldPerc - 'Field in 'inFile' with the derived 'Fire Ignition Potential' percentile rating (i.e. the Fire Ignition Potential Field to be summarized)
# startYear - first year to be processed
# endYear - end year to be processed
# coverType - defined the fire ignition potential relationship type to be applied ('Forest|'Non-Forest') - defines the Percentile values to be used for High, Medium,
# Low fire Danger Ratings

def summarizeFireDangerRating(inFile, inFileFieldAOA, inAOAWildcard, inFieldPerc, startYear, endYear, coverType, statistic, timeStep, rcp, timeField):
    try:

        # Get Table Type .csv or .xlsx
        tableName = os.path.basename(inFile)
        tableSplit = tableName.split(".")
        lenTableSplit = len(tableSplit)
        tableType = str(tableSplit[lenTableSplit - 1])

        # Import excel or csv file to Pandas Dataframe
        if tableType == "csv":
            df = pd.read_csv(inFile)
        else:
            df = pd.read_excel(inFile)

        # Subset the DF to the desired Reocrds (i.e. Forest or Non-Forest Sites)
        df2 = df[df[inFileFieldAOA].str.contains(inAOAWildcard, na=False)]
        # Convert the time variable to a datetime type
        #df2['time'] = pd.to_datetime(df2['time'], utc='None', errors='coerce')
        df2['time'] = pd.to_datetime(df2[timeField], utc='None', errors='coerce')

        del df

         # Summary the Fire Danger Ratings, High, Medium, Low for the defined records, across the defined temporary time frame
        # 1) Subset to the defined year range
        # Convert the start and end year values to a datetime variable

        # Convert the integer startYear and endYear inputs to a DateTime variable
        startYearDT = datetime(year=startYear, month=1, day=1)
        endYearDT = datetime(year=endYear, month=12, day=31)

        # Create initial StartDate, EndDate fieds not UTC aware
        df2['StartDate'] = startYearDT
        df2['EndDate'] = endYearDT

        # Define StartDate and EndDate fields in dataframe as DateTime UTC aware
        df2['StartDate'] = pd.to_datetime(df2['StartDate'], utc='None', errors='coerce')
        df2['EndDate'] = pd.to_datetime(df2['EndDate'], utc='None', errors='coerce')

        # Subset a second  time to the start and end date records
        df3 = df2.loc[(df2['time'] >= df2['StartDate']) & (df2['time'] <= df2['EndDate'])]

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
        df3.set_index(df3['time'], inplace=True)

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
            outDataFrameCur.rename(columns={"time": "timeDrop"}, inplace=True)

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
            outDataFrameCur.rename(columns={"time": "timeDrop"}, inplace=True)

            # Concatenate (i.e. join) Output Dataframe High and Medium
            dfHighMedLow = pd.concat([dfHighMed, outDataFrameCur], axis=1, join='inner')

            # Drop duplicate 'DateTime' field
            dfHighMedLow.drop(['timeDrop'], axis=1, inplace=True)

            # Rename column time to DateTime
            dfHighMedLow.rename(columns={"time": "DateTime"}, inplace=True)
            # Define the SiteName Field in output dataframe
            siteNameLU = df3['SiteName'].values[0]
            dfHighMedLow.insert(loc=0, column='SiteName', value=siteNameLU)
            # Add CoverType Field
            dfHighMedLow.insert(loc=2, column='CoverType', value=coverType)

        # 4) Calculate the MEan Normal from the previously derived annual sums
        if statistic == 'Mean' and timeStep == 'Normal':
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


        return "Success Function", dfNormals
    except:
        messageTime = timeFun()
        print("Error on summarizeFireDangerRating Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'summarizeFireDangerRating'"

# Function defines the fire danger percentile breaks for High, Medium and Low by Cover Type Forest or Non-Forest.  See Figure 6 Thoma et. al. 2020
def fireDangerCategories(coverType):
    try:

        if coverType.lower() == 'forest':
            highFire = 86
            mediumFire = 65

        elif coverType.lower() == 'grassland' or coverType.lower() == 'non-forest':
            highFire = 90
            mediumFire = 73

        else:
            print("'coverType' variable is not defined as 'Forest' or 'Non-Forest' - existing script Failed")
            sys.exit("'coverType' variable is not defined as 'Forest' or 'Non-Forest' - existing script Failed")

        return highFire, mediumFire

    except:
        messageTime = timeFun()
        print("Error on fireDangerCategories Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'fireDangerCategories'"


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


if __name__ == '__main__':

    # Analyses routine ---------------------------------------------------------
    main()
