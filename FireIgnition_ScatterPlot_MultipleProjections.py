
# FireIgnition_ScatterPlot_MultipleProjections
# Script Creates Scatter Plot Summary Figures by Forest and Non-Forest Fire Ignitions Potential
# Scatter Plot includes graphing of the current, historical normals (e.g. 1991-2020), Now Cast, and future projections and ensemble means by RCP 4.6 & 8.5
# Input Files: 1) Table with Summary of Normals (i.e. Historic Normals, and Projections) by High, Medium, Low number of Days - Output from script: FireIgnition_SummarizeScriptNormals.py
# 2)Table with GridMet Station Now Cast Summary and GridMet Station Singular Year summaries by High, Medium, and Low Ignition potential day - Output from script FireIgnitionPotentialNowCastSummarize.py - this script is dynamic and will be run daily to pull the annual data
#
#Output Files:
#1) Scatter Plot Figures with Fire Danger High for Forest Cover and Fire Danger High for Non-Forest Cover by Historic Normal, Last Four Year Singular, Now Cast, and Future Projection Data

#Verion Updates:
#Version 2.0 20220802
#   Added to scatter plots the Yearly value with the Most High Fire Ignition Potential Days as havested from the pervious 25 years
#   Added Jitter to Future Model Graphs
#   Cleaned up legend, title, label definitions
#   Added Z order for graphed content to allow shaded futures box

#Dependicies:
#Python Version 3.9, Numpy, matplotlib, numpy

#Script Name: FireIgnition_ScatterPlot_MultipleProjections
#Created by Kirk Sherrill - Data Manager Rock Mountain Network - I&M National Park Service
#Date: June 30, 2022

##Import Libraries
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as tck

import datetime
from datetime import date
import random
import os

#os.chdir('/var/www/html/ca_backend/python/sherrill')

#Get Current Date
today = date.today()
strDate = today.strftime("%Y%m%d") #Date no Dashes
strDateDash = today.strftime("%Y-%m-%d") #Date with Dashes
strCurrentDate = str(today)  #Date with Dashes
###################################################
# Start of Parameters requiring set up.
###################################################
#Output Folder
outFolder = "C:\\ROMN\\GIS\\FLFO\\LandscapeAnalysis\\FireIgnition\\Python\\SummarizeFLFO\\" + strDate
#outFolder = ""

#Workspace
workspace = "C:\\ROMN\\GIS\\FLFO\\LandscapeAnalysis\\FireIgnition\\Python\\SummarizeFLFO\\" + strDate + "\\workspace"   #workspace
#workspace = ""
#Output Names to Scatter Plots
outNameForest = 'FireDangerHigh_ForestRescaled'
outNameGrass = 'FireDangerHigh_NonForestRescaled'

logFileName = workspace + "FireIgnitionScatterPlots_" + ".LogFile.txt"
#Import Dataset Summary of Normals (i.e. Historic Normals, and Projections) - This dataset should be static is output from FireIgnition_SummaryNormals.py
dfSummaries = pd.read_csv(outFolder + "\FLFO_FireDangerSummary_HistCurrentFutures_Normals" + strDate + ".csv")
#dfSummaries = pd.read_csv(r"C:\ROMN\GIS\FLFO\LandscapeAnalysis\FireIgnition\Python\SummarizeFLFO\20230519\FLFO_FireDangerSummary_HistCurrentFutures_Normals20230519.csv")
#dfSummaries = pd.read_csv(r'./FLFO_FireDangerSummary_HistCurrentFutures_Normals_20220627_v2.csv')

#Import Dataset with GridMet Station Now Cast summary and GridMet Station Singular year summaries - Output from Dailys Gridmet Station Pulls - script 'FireIgnitionPotentialNowCastSummarize.py
dfGridMetNowCast = pd.read_csv(outFolder + "\FireIgnitionNowCastwSummary_" + strDateDash + ".csv")
#dfGridMetNowCast = pd.read_csv(r'C:\ROMN\GIS\FLFO\LandscapeAnalysis\FireIgnition\Python\SummarizeFLFO\20230519\FireIgnitionNowCastwSummary_2023-05-19.csv')  #Gridmet Historic Data - will need to run the 'Gridmet_Historical.py' when a new year is available.
#dfGridMetNowCast = pd.read_csv("FireIgnitionNowCastwSummary.csv")  #Gridmet Historic Data - will need to run the 'Gridmet_Historical.py' when a new year is available.

#######################################
## Below are paths which are hard coded
#######################################

#################################
# Checking for directories and Log File
##################################

if os.path.exists(outFolder):
    pass
else:
    os.makedirs(outFolder)

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


        ############################################
        # Create Datasets Historic lat four years and additional Max Year - Forest
        ############################################
        # Define Last four full years
        today = date.today()
        strDate = today.strftime("%Y/%m/%d")
        strYearNow = int(today.strftime("%Y"))
        startYear = strYearNow - 4
        endYear = strYearNow - 1

        # Define the plotYear field value to be used in the plot
        plotYearNowCast = endYear + 1
        plotYear2031_2060 = endYear + 2
        plotYear2061_2090 = endYear + 3

        # Add Year field to dfGridMetNowCast
        dfGridMetNowCast['Year'] = np.where((dfGridMetNowCast['DateTime'] != 'NowCast_NowCast'), dfGridMetNowCast['DateTime'].str[:4], plotYearNowCast)

        # Make Year Numeric
        dfGridMetNowCast['Year'] = pd.to_numeric(dfGridMetNowCast['Year'], errors='coerce')

        # Define Plot Year
        dfSummaries['plotYear'] = np.where((dfSummaries['DateTime'] == '2031_2060'), plotYear2031_2060,
                                           np.where((dfSummaries['DateTime'] == '2061_2090'), plotYear2061_2090, 999))
        # Make plotYear Numeric
        dfSummaries['plotYear'] = pd.to_numeric(dfSummaries['plotYear'], errors='coerce')

        ##############################
        # Get Max Year Info - Forest
        dfHistoricForestTemp = dfGridMetNowCast.loc[(dfGridMetNowCast['CoverType'] == 'Forest')]

        # Find Max Year - Forest
        maxHighForest = dfHistoricForestTemp['High_Mean'].max()

        # Subset Df to the max year Record
        indexDf = dfHistoricForestTemp.loc[dfHistoricForestTemp['High_Mean'] == maxHighForest]

        # Extract the Max Year
        maxYearForest = int(indexDf['Year'].to_string(index=False))
        del (dfHistoricForestTemp)

        ###############################################
        # Create List of Years to be processed - Forest
        rangeListForest = [*range(startYear, endYear + 1)]
        # Add Max Year Forest
        rangeListForest.insert(0, maxYearForest)

        # Forest
        # Loop of year's to be processed add the expected Syntax for the 'DateTime' field designation ofthe year range (i.e year_year)
        yearListForest = []
        xSeriesTicsForest = []  # List for Graph Tics
        xSeriesLabelForest = []  # Labels List
        for year in rangeListForest:
            yearListForest.append(str(year) + "_" + str(year))
            xSeriesTicsForest.append(year)
            xSeriesLabelForest.append(year)

        # Add Projection Label Info x Tic marks and Labels
        xSeriesTicsForest.append(plotYearNowCast)
        xSeriesTicsForest.append(plotYear2031_2060)
        xSeriesTicsForest.append(plotYear2061_2090)
        xSeriesLabelForest.append(strDate)
        xSeriesLabelForest.append('2031-2060')
        xSeriesLabelForest.append('2061-2090')

        # Define the minus 1 start Year to be used for the Max High Year
        startYearMinusOne = startYear - 1

        # Replace Year value for the Max High Year with the 'startYearMinusOne'
        xSeriesTicsForest[0] = startYearMinusOne

        ##############################
        # Get Max Year Info - NonForest
        dfHistoricNonForestTemp = dfGridMetNowCast.loc[(dfGridMetNowCast['CoverType'] == 'Non-Forest')]

        # Find Max Year - Forest
        maxHighNonForest = dfHistoricNonForestTemp['High_Mean'].max()

        # Subset Df to the max year Record
        indexDf = dfHistoricNonForestTemp.loc[dfHistoricNonForestTemp['High_Mean'] == maxHighNonForest]

        # Extract the Max Year
        maxYearNonForest = int(indexDf['Year'].to_string(index=False))
        del (dfHistoricNonForestTemp)

        ###############################################
        # Create List of Years to be processed - NonForest
        rangeListNonForest = [*range(startYear, endYear + 1)]
        # Add Max Year Forest
        rangeListNonForest.insert(0, maxYearNonForest)

        # Forest
        # Loop of year's to be processed add the expected Syntax for the 'DateTime' field designation ofthe year range (i.e year_year)
        yearListNonForest = []
        xSeriesTicsNonForest = []  # List for Graph Tics
        xSeriesLabelNonForest = []  # Labels List
        for year in rangeListNonForest:
            yearListNonForest.append(str(year) + "_" + str(year))
            xSeriesTicsNonForest.append(year)
            xSeriesLabelNonForest.append(year)

        # Add Projection Label Info x Tic marks and Labels
        xSeriesTicsNonForest.append(plotYearNowCast)
        xSeriesTicsNonForest.append(plotYear2031_2060)
        xSeriesTicsNonForest.append(plotYear2061_2090)
        xSeriesLabelNonForest.append(strDate)
        xSeriesLabelNonForest.append('2031-2060')
        xSeriesLabelNonForest.append('2061-2090')

        # Define the minus 1 start Year to be used for the Max High Year
        startYearMinusOne = startYear - 1

        # Replace Year value for the Max High Year with the 'startYearMinusOne'
        xSeriesTicsNonForest[0] = startYearMinusOne

        ###################################################
        # Subset last four year singular summaries - Gridmet
        dfHistoricForest = dfGridMetNowCast.loc[
            (dfGridMetNowCast['DateTime'].isin(yearListForest)) & (dfGridMetNowCast['CoverType'] == 'Forest')]
        dfHistoricNonForest = dfGridMetNowCast.loc[
            (dfGridMetNowCast['DateTime'].isin(yearListNonForest)) & (dfGridMetNowCast['CoverType'] == 'Non-Forest')]

        ########
        # Get Now Cast By Cover Type
        dfNowCastForest = dfGridMetNowCast.loc[
            (dfGridMetNowCast['NowCastCount'] >= 0) & (dfGridMetNowCast['CoverType'] == 'Forest')]
        dfNowCastNonForest = dfGridMetNowCast.loc[
            (dfGridMetNowCast['NowCastCount'] >= 0) & (dfGridMetNowCast['CoverType'] == 'Non-Forest')]

        ############################################
        # Create Datasets to be used in plot -Forest Projections
        ############################################
        dfForest_RCP45 = dfSummaries.loc[(dfSummaries['CoverType'] == 'Forest') & (dfSummaries['RCP'] == 'rcp45') & (
                    dfSummaries['GCM'] != 'Ensemble')]
        dfForest_RCP85 = dfSummaries.loc[(dfSummaries['CoverType'] == 'Forest') & (dfSummaries['RCP'] == 'rcp85') & (
                    dfSummaries['GCM'] != 'Ensemble')]
        dfForest_RCP45_Ensemble = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Forest') & (dfSummaries['RCP'] == 'rcp45') & (
                        dfSummaries['GCM'] == 'Ensemble')]
        dfForest_RCP85_Ensemble = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Forest') & (dfSummaries['RCP'] == 'rcp85') & (
                        dfSummaries['GCM'] == 'Ensemble')]
        dfForest_1991_2020 = dfSummaries.loc[(dfSummaries['CoverType'] == 'Forest') & (dfSummaries['RCP'] == 'na')]

        # Define Jitter fields for Projections
        dfForest_RCP45['plotYearJitter'] = dfForest_RCP45['plotYear'].apply(lambda x: jitter(x))
        dfForest_RCP85['plotYearJitter'] = dfForest_RCP85['plotYear'].apply(lambda x: jitter(x))

        ############################################
        # Create Datasets to be used in plot -Non Forest Projections
        ############################################
        dfNonForest_RCP45 = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Non-Forest') & (dfSummaries['RCP'] == 'rcp45') & (
                        dfSummaries['GCM'] != 'Ensemble')]
        dfNonForest_RCP85 = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Non-Forest') & (dfSummaries['RCP'] == 'rcp85') & (
                        dfSummaries['GCM'] != 'Ensemble')]
        dfNonForest_RCP45_Ensemble = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Non-Forest') & (dfSummaries['RCP'] == 'rcp45') & (
                        dfSummaries['GCM'] == 'Ensemble')]
        dfNonForest_RCP85_Ensemble = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Non-Forest') & (dfSummaries['RCP'] == 'rcp85') & (
                        dfSummaries['GCM'] == 'Ensemble')]
        dfNonForest_1991_2020 = dfSummaries.loc[
            (dfSummaries['CoverType'] == 'Non-Forest') & (dfSummaries['RCP'] == 'na')]

        # Define Jitter fields for Projections
        dfNonForest_RCP45['plotYearJitter'] = dfNonForest_RCP45['plotYear'].apply(lambda x: jitter(x))
        dfNonForest_RCP85['plotYearJitter'] = dfNonForest_RCP85['plotYear'].apply(lambda x: jitter(x))

        # Get Number of Now Cast Days
        nowCastDays = dfNowCastNonForest['NowCastCount'].iloc[0]
        nowCastDaysStr = "Short Term Forecast (" + str(int(nowCastDays)) + " days)"

        ######################################################################
        # Forest Scatter Plot with High Count Year in last 25 added - KRS 20220802
        ######################################################################

        # Define the MaxYear DataFrame
        dfHistoricForestMax = dfHistoricForest.loc[(dfHistoricForest['Year'] == maxYearForest)]

        # Assign Desired Year value to facilitate x axis mapping
        dfHistoricForestMax['Year'] = np.where((dfHistoricForestMax['Year'] == maxYearForest), startYearMinusOne,
                                               dfHistoricForestMax['Year'])

        # Drop Max Year from 'dfHistoricForest' dataframe
        dfHistoricForest = dfHistoricForest[1:]

        ax = dfHistoricForestMax.plot(kind='scatter', x='Year', y='High_Mean',  color='Black', marker = "^",
                                      label='High Annual Count Last 25 Years', zorder=10)
        dfHistoricForest.plot(kind='scatter', x='Year', y='High_Mean', color='Black',
                              label='Annual Count in Past Years', ax=ax, zorder=9)
        dfNowCastForest.plot(kind='scatter', x='Year', y='High_Mean', color='Orange', label=nowCastDaysStr, ax=ax, zorder=8)

        # Get X axis min value - Futures
        xlow = dfForest_RCP45['plotYearJitter'].min()

        #Get X axis max value - Futures
        xlim = dfForest_RCP85['plotYearJitter'].max()

        # Add Shading for the Futures area
        ax.axvspan(xmin=xlow - 0.2, xmax=xlim + 0.2, ymin=0, linewidth=8, color='lightskyblue', zorder=0)


        # First Dataset RCP 4.5 - No Ensemble
        # dfForest_RCP45.plot(kind='scatter', x='plotYear', y='High_Mean', color='Blue', label='Low Emission Model Predictions (RCP4.5)', ax=ax);
        dfForest_RCP45.plot(kind='scatter', x='plotYearJitter', y='High_Mean', color='Blue',
                            label='Low Emission Model Predictions (RCP4.5)', ax=ax, zorder=7)

        # Add Second Dataset RCP 8.5 - No Ensemble
        # dfForest_RCP85.plot(kind='scatter', x='plotYear', y='High_Mean', color='Red', label='High Emission Model Predictions (RCP8.5)', ax=ax)
        dfForest_RCP85.plot(kind='scatter', x='plotYearJitter', y='High_Mean', color='Red',
                            label='High Emission Model Predictions (RCP8.5)', ax=ax, zorder=6)

        # Third Dataset RCP 4.5 - Ensemble
        dfForest_RCP45_Ensemble.plot(kind='scatter', x='plotYear', y='High_Mean', color='Blue',
                                     label='Low Emission Models Mean', s=150, ax=ax, zorder=11);

        # Fourth Dataset RCP 8.5 - Ensemble
        dfForest_RCP85_Ensemble.plot(kind='scatter', x='plotYear', y='High_Mean', color='Red',
                                     label='High Emission Model Mean', s=150, ax=ax, zorder=12);

        plt.xticks(xSeriesTicsForest, xSeriesLabelForest)
        plt.ylabel("Number of Days")
        # plt.xlabel("Year - Current Date Now Cast")
        plt.xlabel("Year - Current Date - Futures\n\n\
        Number of days annually with a High Fire Danger Ignition Potential rating for forested landcover:\n\
         at time steps: highest annual count last 25 years, historic mean (1991-2020), last four years, \n\
        short term forecasts (30-60 days future) and climate future predictions (2031-2060, and 2061-2090) \n\
        across 11 Global Circulation Models (GCM) and ensemble means across all GCMs by Representative \n\
        Concentration Pathways (RCP) carbon scenarios 4.5 (low emissions) and 8.5 (high emissions).")

        # Define the 1991-2020 Normal Value - From Grid Met Station
        normal1991_2020 = dfForest_1991_2020['High_Mean']
        # Make float
        normal1991_20120lt = float(normal1991_2020)

        # Add Dataset 1990-2019 Normal value
        # ax.axhline(y=normal1991_20120lt, color='Black', xmin=0, xmax=0.6, label='Normal 1991-2020')  #With item in Legend
        ax.axhline(y=normal1991_20120lt, color='Black', xmin=0, xmax=0.6, zorder=13)

        # Define location for Normal Label Placement - New
        curYearPlus1 = dfHistoricForest['Year'].iat[0] + 0.25

        # Add Normals Line Text - New
        ax.text(curYearPlus1, normal1991_20120lt + 1, 'Historic Mean 1991-2020', color='Black', rotation=360)

        plt.legend(loc='upper right', fontsize = 'small', borderaxespad=0.2, facecolor ="white", framealpha = 1.0)
        plt.title("High Fire Danger - Forest")

        today = date.today()
        strDate = today.strftime("%Y-%m-%d")
        # Define the Figure Size
        figure = plt.gcf()
        figure.set_size_inches(10, 7.5)
        plt.savefig(outNameForest + ".jpg", dpi=100, bbox_inches='tight')
        del (figure)

        ######################################################################
        # NonForest Scatter Plot with High Count Year in last 25 added - KRS 20220802
        ######################################################################

        # Define the MaxYear DataFrame
        dfHistoricNonForestMax = dfHistoricNonForest.loc[(dfHistoricNonForest['Year'] == maxYearNonForest)]

        # Assign Desired Year value to facilitate x axis mapping
        dfHistoricNonForestMax['Year'] = np.where((dfHistoricNonForestMax['Year'] == maxYearNonForest),
                                                  startYearMinusOne, dfHistoricNonForestMax['Year'])

        # Drop Max Year from 'dfHistoricNonForest' dataframe
        dfHistoricNonForest = dfHistoricNonForest[1:]

        ax = dfHistoricNonForestMax.plot(kind='scatter', x='Year', y='High_Mean', color='Black', marker = "^",
                                         label='High Annual Count Last 25 Years', zorder=10)
        dfHistoricNonForest.plot(kind='scatter', x='Year', y='High_Mean', color='Black',
                                 label='Annual Count Past Years', ax=ax)
        dfNowCastNonForest.plot(kind='scatter', x='Year', y='High_Mean', color='Orange', label=nowCastDaysStr, ax=ax, zorder=9)

        #Get X axis max value - Futures
        xlow = dfNonForest_RCP45['plotYearJitter'].min()

        #Get X axis max value - Futures
        xlim = dfNonForest_RCP85['plotYearJitter'].max()

        # Add Shading for the Futures area
        ax.axvspan(xmin=xlow - 0.2, xmax=xlim + 0.2, ymin=0, linewidth=8, color='lightskyblue', zorder=0)


        #First Dataset RCP 4.5 - No Ensemble
        # dfNonForest_RCP45.plot(kind='scatter', x='plotYear', y='High_Mean', color='Blue', label='Low Emission Model Predictions (RCP4.5)', ax=ax);
        dfNonForest_RCP45.plot(kind='scatter', x='plotYearJitter', y='High_Mean', color='Blue',
                               label='Low Emission Model Predictions (RCP4.5)', ax=ax, zorder=7);

        # Add Second Dataset RCP 8.5 - No Ensemble
        # dfNonForest_RCP85.plot(kind='scatter', x='plotYear', y='High_Mean', color='Red', label='High Emission Model Predictions (RCP8.5)', ax=ax)
        dfNonForest_RCP85.plot(kind='scatter', x='plotYearJitter', y='High_Mean', color='Red',
                               label='High Emission Model Predictions (RCP8.5)', ax=ax, zorder=6)

        # Third Dataset RCP 4.5 - Ensemble
        dfNonForest_RCP45_Ensemble.plot(kind='scatter', x='plotYear', y='High_Mean', color='Blue',
                                        label='Low Emission Model Mean', s=150, ax=ax, zorder=11);

        # Fourth Dataset RCP 8.5 - Ensemble
        dfNonForest_RCP85_Ensemble.plot(kind='scatter', x='plotYear', y='High_Mean', color='Red',
                                        label='High Emission Model Mean', s=150, ax=ax, zorder=12);

        plt.xticks(xSeriesTicsNonForest, xSeriesLabelNonForest)
        plt.ylabel("Number of Days")
        # plt.xlabel("Year - Current Date Now Cast")
        plt.xlabel("Year - Current Date - Futures\n\n\
        Number of days annually with a High Fire Danger Ignition Potential rating for grassland and shrub\n\
        landcover at time steps: highest annual count last 25 years, historic mean (1991-2020), last four years,\n\
        short term forecasts (30-60 days future) and climate future predictions (2031-2060, and 2061-2090) \n\
        across 11 Global Circulation Models (GCM) and ensemble means across all GCMs by Representative \n\
        Concentration Pathways (RCP) carbon scenarios 4.5 (low emissions) and 8.5 (high emissions).")

        # Define the 1991-2020 Normal Value - From Grid Met Station
        normal1991_2020 = dfNonForest_1991_2020['High_Mean']
        # Make float
        normal1991_20120lt = float(normal1991_2020)

        # Add Dataset 1990-2019 Normal value
        # ax.axhline(y=normal1991_20120lt, color='Black', xmin=0, xmax=0.6, label='Normal 1991-2020')  #With item in Legend
        ax.axhline(y=normal1991_20120lt, color='Black', xmin=0, xmax=0.6, zorder=13)

        # Define location for Normal Label Placement - New
        curYearPlus1 = dfHistoricNonForest['Year'].iat[0] + 0.25

        # Add Normals Line Text - New
        ax.text(curYearPlus1, normal1991_20120lt + 1, 'Historic Mean 1991-2020', color='Black', rotation=360)

        plt.legend(loc='upper right', fontsize = 'small', borderaxespad=0.2, facecolor ="white", framealpha = 1.0)
        plt.title("High Fire Danger - Grassland and Shrub")

        today = date.today()
        strDate = today.strftime("%Y-%m-%d")
        # Define the Figure Size
        figure = plt.gcf()
        figure.set_size_inches(10, 7.5)
        plt.savefig(outNameGrass + ".jpg", dpi=100, bbox_inches='tight')
        del (figure)

        messageTime = timeFun()
        scriptMsg = "Successfully processed Fire Ignition Scatter Plots: " + outFolder + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")



    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - FireIgnition_ScatterPlots - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

# Function to Get the Date/Time
def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime

#Function to apply random variance in X axis placement - i.e. a jitter effect
def jitter(x):
    return x + random.uniform(0, .5) -.25

if __name__ == '__main__':

    # Analyses routine ---------------------------------------------------------
    main()
