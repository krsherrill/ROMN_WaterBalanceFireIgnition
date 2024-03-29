# ROMN_WaterBalanceFireIgnition
National Park Service Rocky Mountain Network Water Balance Fire Ignition Model Scripts.  These scripts apply a Water Balance based Fire Ignition Model at the follow time steps: Historic (1991-2020 normal), last four years (e.g. 2018-2021), near term NowCasts (i.e. short term future forecasts ~30-60 days) and climate projection normals (2031-2060, and 2061-2090) across 11 Global Circulation Models (GCM) and an ensemble mean across all GCMs by Representative Concentration Pathways (RCP) climate scenarios 4.5 (low emission) and 8.5 (high emission).

**Output** - Graph with the number of days annually with a High Fire Danger rating for Forested and NonForested landcover at the above time steps.  A High Fire Danger rating was defined as having a fire ignition potential >= to the 86th (forest) and 90th (non-forest) percentile water balance deficit value across fires greater then 405 ha between 1984-2005 in the Southern Rocky Mountain Ecoregion. For additional details on the water balance based fire ignition model see Thoma et. al. 2020 at https://doi.org/10.1016/j.gecco.2020.e01300. 

**Data Sources** Historic and Now Cast water balance deficit data is from the a defined Grid Met Station available on Climate Analyzer - http://www.climateanalyzer.us/ . Future projection data spatially coincident with the defined Grid Met Station is obtained from NPS Water Balance data version 1.5 at: http://www.yellowstone.solutions/thredds/catalog.html.
 
## 1) FireIgnitionRaw_GridMet_Historic.py
Scripts Derives Fire Ignition Potential and categorizes By High, Medium, and Low Fire Ignition Potential rating pulling from the defined GridMet Station in Climate Analyzer. Fire Ignition Output is from 1991-2020 at a daily time step. For a station/location this will only need to be ran once.
## 2) GCM_wb_thredds_point_extractor_v3.py
Script Extracts NPS Water Balance projection data (version 1.5) from the http://www.yellowstone.solutions/thredds Threads Server. Script output is used in the *FireIgnitionRaw_Projections.py* script. For a station/location this will only need to be ran once.
## 3) FireIgnitionRaw_Projections.py
Scripts Derives Futures Fire Ignition Potential and categorizes By High, Medium, and Low Fire Ignition Potential rating at the defined point location using NPS Water Balance Data future projection Water Balance data as input.  The temporal range to be processed is determined by the input projections futures Water Balance data being processed.  The Input Futures NPS Water Balance data (Version 1.5) is pulled from the http://www.yellowstone.solutions/thredds Threads Server via script *GCM_wb_thredds_point_extractor_v3.py*.   For a station/location this will only need to be ran once.
## 4) FireIgnition_SummaryNormals.py
Script applies the High, Medium and Low Fire Ignition model classification by Fire Ignition Model (Thoma et. al. 2020) Land Cover Type (i.e. Forest and Non-Forest) across defined temporal ranges.  Subsequently processing summarizes this classification across a defined temporal period which is defiend via the *HistoricCurrentProcessingList* table.  Summary periods are usually by normals periods (e.g. Historic: 1991-2020, Futures 2031-2060, 2061-2090, etc.). For a station/location this will only need to be ran once.
## 5) FireIgnitionPotentialNowCastSummarize.py
Scripts Derives Fire Ignition Potential and categorizes By High, Medium, and Low Fire Ignition Potential for short term/now cast data at the defined GridMet Station in Climate Analyzer. For a station/location script will be ran daily to pull in the most current daily and nowcast data.
## 6) FireIgnition_ScatterPlot_MultipleProjections.py
Final Script in the Fire Ignition workflow. Creates Scatter Plot Summary Figures by Forest and Non-Forest Fire Ignitions Potential.
Scatter Plot includes graphing of the current, historical normals (e.g. 1991-2020), Now Cast, and future projections and ensemble means by RCP 4.6 & 8.5. For a station/location script will be ran daily to pull in the most current daily and nowcast data. Input Files: 

- Table with Summary of Normals (i.e. Historic Normals, and Projections) by High, Medium, Low number of Days - Output from script: *FireIgnition_SummarizeScriptNormals.py*.

- Table with GridMet Station Now Cast Summary and GridMet Station Singular Year summaries by High, Medium, and Low Ignition potential day - Output from script *FireIgnitionPotentialNowCastSummarize.py*. 

An example Fire Ignition Potential Model output graph for the Forested Landcover model is show below:
![Example Fire Ignition Potential Model output graph for the Forested Landcover model.](FireDangerHigh_ForestRescaled.jpg)
