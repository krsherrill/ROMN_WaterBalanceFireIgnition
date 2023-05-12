
# ---------------------------------------------------------------------------
# FLFO_FireIgnitionRaw_Projections.py
#Script to Derive Fire Iginition Potential at from Water Balance Projection Data.
#Applies the Fire Iginition Potential as defined in Thoma et. al. 2020 Management Applications Paper
#Input Paraemeter is Cumulative Water Defict - Water Balance paraemeter - Data is being
#Code performs the following routines to derive Fire Ignition Potential:
#1) Subsets the input table to the desired fields and exports to a Comma delimited text file defining the 'dataset'

# Dependicies:
# Futures/Projections Water Balance Data is pulled from the NPS Water Balance Data (version 1.5) on the
# http://www.yellowstone.solutions/thredds threads server via the  GCM_wb_thredds_point_extractor_v3.py script.

#Python Version 3.9, Numpy. Pandas
#Created by Kirk Sherrill - Data Manager Rock Mountian Network - I&M National Park Service
#Date - October 1st, 2021

#Script Name: FLFO_FireIgnitionRaw_Projections.py  - was previously called 'FLFO_FireIgnitionRaw_Projections_20220922.py' locally on KRS
#Created by Kirk Sherrill - Data Manager Rock Mountain Network - I&M National Park Service

###################################################
# Start of Parameters requiring set up.
###################################################

##Import Libraries
import pandas as pd, traceback, sys, os
import numpy as np
import datetime
from datetime import date

#Projections/Future Variables
inFileProjections = r"C:\ROMN\GIS\FLFO\LandscapeAnalysis\WaterBalance\Projections\SingleForestGrassland\MergedAll\FLFO_SingleForestGrassland_WB_Daily_Deficit_AllPRJ_2220_2099v2b_wEnsmbAvgGrassOnly.csv"   #File with Futures Projections data Water Balance Data
inFileTimeProj = "time"    #Time Field in 'in projection data file
uniqueInFileProj = "SiteName"   #Field with the unique identifier in projection data file
movingWindowsDay = 14  #Number of days in the moving window average (default use 14)


#Define the Historic/Current reference parameters:
refYearStartDate= '1/1/1984'   #Start Year/Date for which Fire Ignition Model was evaluated (Jan 1 of Start Year)
refYearEndDate = '12/31/2005'       #End Year/Date for which Fire Ignition Model was evaluated (Dec 31 of End Year

ProjectionLoop = ['deficit_CanESM2_rcp45', 'deficit_CanESM2_rcp85', 'deficit_CCSM4_rcp45', 'deficit_CCSM4_rcp85', 'deficit_CNRM-CM5_rcp45', 'deficit_CNRM-CM5_rcp85', 'deficit_CSIRO-Mk3-6-0_rcp45',
                  'deficit_CSIRO-Mk3-6-0_rcp85', 'deficit_GFDL-ESM2G_rcp45', 'deficit_GFDL-ESM2G_rcp85', 'deficit_HadGEM2-CC365_rcp45', 'deficit_HadGEM2-CC365_rcp85', 'deficit_inmcm4_rcp45', 
                  'deficit_inmcm4_rcp85', 'deficit_IPSL-CM5A-LR_rcp45', 'deficit_IPSL-CM5A-LR_rcp85', 'deficit_MIROC5_rcp45', 'deficit_MIROC5_rcp85', 'deficit_MRI-CGCM3_rcp45',
                  'deficit_MRI-CGCM3_rcp85', 'deficit_NorESM1-M_rcp45', 'deficit_NorESM1-M_rcp85', 'deficit_Ensemble_rcp45', 'deficit_Ensemble_rcp85']   #List Defining the Projections in 'inFileProjections' to be processed


#Get Current Date
today = date.today()
strDate = today.strftime("%Y%m%d")

#Output Directory/LogFile Information
outputFolder = r"C:\ROMN\GIS\FLFO\LandscapeAnalysis\FireIgnition\Python\SummarizeFLFO\\" + strDate #Folder for the output Data Package Products
#outputFolder = "./"  #Folder for the output Data Package Products
workspace = outputFolder + "\\workspace"   #workspace
#workspace = "./"  #workspace
outName = 'FireIgnitionRating_Futures'   #Output .csv filename
logFileName = workspace + "\\" + outName + ".LogFile.txt"


#Fire Year Parameters by Vegtation Type
nonforest_start_DOY = 74  #Non-Forested Start of Fire Year Day Number
nonforest_end_DOY = 301   #Non-Forested End of Fire Year Day Number
forest_start_DOY = 7      #Forest Start of Fire year Day Number
forest_end_DOY = 303      #Forest End of Fire year Day Number

#######################################
## Below are paths which are hard coded
#######################################

#################################
# Checking for working directories and Log File
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

#################################################
##
def main():

    try:

        #################################################################
        #Run Functions for the Futures Projections GCM and RCPs Scenarios
        #################################################################

        loopCount = 1
        for val in ProjectionLoop:
            #Define the GCM being processed
            GCM_RCP_Field = str(val)

            #Create Reference List for Forested Vegetation Type
            outVal = define_ReferenceList(forest_start_DOY, forest_end_DOY, refYearStartDate, refYearEndDate, inFileProjections, GCM_RCP_Field, inFileTimeProj, 'yes')
            if outVal[0] != "Success function":
                print("WARNING - Function define_ReferenceList failed - Exiting Script")
                exit()
            else:
                print("Success - Function define_ReferenceList - Forested Vegetation")
                # Assign the reference Data Frame
                refDF_ForestProject = outVal[1]

            # Create Reference List for NonForest - Grassland Vegetation Type
            outVal = define_ReferenceList(nonforest_start_DOY, nonforest_end_DOY, refYearStartDate, refYearEndDate, inFileProjections, GCM_RCP_Field, inFileTimeProj, 'yes')
            if outVal[0] != "Success function":
                print("WARNING - Function define_ReferenceList failed - Exiting Script")
            else:
                print("Success - Function define_ReferenceList - Non Forest Vegetation")
                # Assign the reference Data Frame
                refDF_NonForestProject = outVal[1]

            # Define Moving Averages Futures/Projections
            outVal = define_MovingWindowAverage(inFileProjections, movingWindowsDay, GCM_RCP_Field)
            if outVal[0] != "Success function":
                print("WARNING - Function define_MovingWindowAverage failed - Exiting Script")
            else:
                print("Success - Function define_MovingWindowAverage - for " + inFileProjections)
                # Assign output to DF
                dfProject_wAvg = outVal[1]

            # Define Percentiles - for Projections, and using Forest Fire Season
            outVal = define_IgnitionProportion(dfProject_wAvg, GCM_RCP_Field, refDF_ForestProject, GCM_RCP_Field, "PercForest" + GCM_RCP_Field, "PropFiresForest" + GCM_RCP_Field, "Forest")
            if outVal[0] != "Success function":
                print("WARNING - Function define_IgnitionProportion failed - for 'Projections - " + GCM_RCP_Field + " - Exiting Script")
            else:
                print("Success - Function define_IgnitionProportion - for 'Projections - " + GCM_RCP_Field + " - Forest")
                if loopCount == 1:
                    print("Success - Function define_IgnitionProportion - for 'Projections - " + GCM_RCP_Field + " - Forest")
                else:
                     # Append the Percent and Proportion Fire Fields to the Master Data Frame 'outPrevProj'
                    outPrevProj["PercForest" + GCM_RCP_Field] = outVal[1]["PercForest" + GCM_RCP_Field]
                    outPrevProj["PropFiresForest" + GCM_RCP_Field] = outVal[1]["PropFiresForest" + GCM_RCP_Field]

            # Define Percentiles - for Projections, and using NonForest Fire Season
            if loopCount == 1:
                outVal = define_IgnitionProportion(dfProject_wAvg, GCM_RCP_Field, refDF_NonForestProject, GCM_RCP_Field, "PercNonForest" + GCM_RCP_Field, "PropFiresNonForest" + GCM_RCP_Field, "NonForest")
            else:
                outVal = define_IgnitionProportion(dfProject_wAvg, GCM_RCP_Field, refDF_NonForestProject, GCM_RCP_Field, "PercNonForest" + GCM_RCP_Field, "PropFiresNonForest" + GCM_RCP_Field, "NonForest")

            if outVal[0] != "Success function":
                print("WARNING - Function define_IgnitionProportion failed - for 'Projections - " + GCM_RCP_Field + " - Exiting Script")
            else:
                print("Success - Function define_IgnitionProportion - for 'Projections - " + GCM_RCP_Field + " - NonForest")
                if loopCount == 1:
                    #Assign dataframe which will be appended to on subsequent RCP iterations
                    outPrevProj = outVal[1]
                else:
                    # Append the Percent and Proportion Fire Fields to the Master Data Frame 'outPrevProj'
                    outPrevProj["PercNonForest" + GCM_RCP_Field] = outVal[1]["PercNonForest" + GCM_RCP_Field]
                    outPrevProj["PropFiresNonForest" + GCM_RCP_Field] = outVal[1]["PropFiresNonForest" + GCM_RCP_Field]
            messageTime = timeFun()
            scriptMsg = "Successfully processed Projection - " + GCM_RCP_Field + " + " + messageTime
            print (scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")

            loopCount += 1

        #Export Dataframe: outPrevProj
        outputFull2 = outputFolder + "\\" + outName + "_" + strDate + ".csv"
        # Export Output to csv
        outPrevProj.to_csv(outputFull2, ",")
        messageTime = timeFun()
        print("Successfully finished Processing - " + messageTime)

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

# Function Creates a new DataFrame from the defined Columns/Series in a pre-existing dataframe
def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame

###############################################################################################################
#Function 1. Routine to Derive the Historical 1984-2015 Fire Period Water Deficit full list for the Fire Period
###############################################################################################################
# startDate - First date of fire period across the 1984-2015 fire period
# enddate - Last date of fire period across the 1984-2015 fire period
# inputVariableList - List of the climatic variable being evaluated (e.g. Water Balance - Deficit)
# inFileField - Parameter being evaluated (e.g. Deficit)
# inFileTime - Parameter defined the time field

# Output - pandas data series (i.e. 1D) with the input variable (e.g. Deficit) trimmed to the defined fire year range
def define_ReferenceList(startDate, endDate, refYearStartDate, refYearEndDate, inputVariableList, inFileField, inFileTime, futures):
    try:

        'Make data frame from Input List'
        df = pd.read_csv(inputVariableList)

        selected_columns = []
        selected_columns.append(inFileTime)
        selected_columns.append(inFileField)

        # Function to take the defined Columns and Export the Columns to a new Pandas Data Frame
        df2 = select_columns(df, selected_columns)  # Creating new data frome with only the date and time series field being processed

        #Create 'timeDate' field as datetime filed
        df2["timeDate"] = pd.to_datetime(df2[inFileTime])

        # Create the Day of Year Field
        df2["DOY"] = df2['timeDate'].dt.dayofyear

        # Select where >=Start Date and <= End Date
        outDfFireYear1 = df2[(df2['DOY'] >= startDate) & (df2['DOY'] <= endDate)]

        # Create 'timeDate' field as datetime filed
        #outDfFireYear1["timeDate"] = pd.to_datetime(outDfFireYear1["timeDate"])

        if futures.lower() == "yes":
            outDfFireYear = outDfFireYear1

        else: #not processing futures add year subset for 1984-2005
            # Select Second subset fire years of interest Year >= refYearStart Date and <= refYearEnd  0- Added 20220627
            # Convert Inputs to DateTime parameters
            refYearStartDT = pd.to_datetime(refYearStartDate, format='%m/%d/%Y')
            refYearEndDT = pd.to_datetime(refYearEndDate, format='%m/%d/%Y')

            outDfFireYear = outDfFireYear1[(outDfFireYear1['timeDate'] >= refYearStartDT) & (outDfFireYear1['timeDate'] < refYearEndDT)]

        # Reset Index to 0
        outDfFireYear.reset_index(drop=True, inplace=True)

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
# - inDataSet: Dataset which is being evaluated (e.g. Historical/Current, Future Projects)
# - movingWindowDays: Moving window number of days prior to the date to be derived (Default will be 14 day prior)
# - fieldToAverage: Field in the 'inDateSet' being averaged
# Output - New Field -'MovingAverage_{movingWindowDays}' in output dataframe with the moving window Average

# Note in Rolling/Moving window average min_periods is set to 1 (i.e. only 1 day of data needed)
def define_MovingWindowAverage(inDataSet, movingWindowDays, fieldToAverage):
    try:

        # Make data frame from Input
        df = pd.read_csv(inDataSet)

        outField = fieldToAverage + "_" + str(movingWindowDays)
        # Use Rolling in dataframe to calculate the Moving Window Average
        df[outField] = df[fieldToAverage].rolling(movingWindowDays, min_periods=1, win_type=None).mean()

        return "Success function", df
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
    f = 0.047 * np.e**(0.075*percentile)
    return f

#Forest Equation where percentile is the percentile and f is the percent of historic fires (1984-2015) middle rockies
#that has ignited under the percentile dryness conditions
def forest_equation(percentile):
    f = 0.368 * np.e**(0.055*percentile)
    return f

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
