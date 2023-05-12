# ROMN_WaterBalanceFireIgnition
National Park Service Rocky Mountain Network Water Balance Fire Ignition Model Scripts.  These scripts apply a Water Balance based Fire Ignition Model at the follow time steps: Historic (1991-2020 normal), last four years (e.g. 2018-2021), near term NowCasts (i.e. short term future forecasts ~30-60 days) and climate projection normals (2031-2060, and 2061-2090) across 11 Global Circulation Models (GCM) and an ensemble mean across all GCMs by Representative Concentration Pathways (RCP) climate scenarios 4.5 (low emission) and 8.5 (high emission).

**Output** - Graph with the number of days annually with a High Fire Danger rating for Forested and NonForested landcover at the above time steps.  A High Fire Danger rating was defined as having a fire ignition potential >= to the 86th (forest) and 90th (non-forest) percentile water balance deficit value across fires greater then 405 ha between 1984-2005 in the Southern Rocky Mountain Ecoregion. For additional details on the water balance based fire ignition model see Thoma et. al. 2020 at https://doi.org/10.1016/j.gecco.2020.e01300. 

**Data Sources** Historic and Now Cast water balance deficit data is from the a defined Grid Met Station available on Climate Analyzer - http://www.climateanalyzer.us/ . Future projection data spatially coincident with the defined Grid Met Station is obtained from NPS Water Balance data version 1.5 at: http://www.yellowstone.solutions/thredds/catalog.html.
 

## FireIgnitionRaw_GridMet_Historic.py
Scripts Derives Fire Ignition Potential and categorizes By High, Medium, and Low Fire Ignition Potential rating pulling from the defined GridMet Station in Climate Analyzer. Fire Ignition Output is from 1991-2020 at a daily time step.

## FireIgnitionRaw_Projections.py
Scripts Derives Futures Fire Ignition Potential and categorizes By High, Medium, and Low Fire Ignition Potential rating at the defined point location using NPS Water Balance Data future projection Water Balance data as input.  The temporal range to be process is determined by the input projections futures Water Balance data being processed.  The Input Futures NPS Water Balance data (Version 1.5) is pulled from the http://www.yellowstone.solutions/thredds Threads Server via script *GCM_wb_thredds_point_extractor_v3.py*.  

## GCM_wb_thredds_point_extractor_v3.py
Script Extracts NPS Water Balance projection data (version 1.5) from the http://www.yellowstone.solutions/thredds Threads Server. Script output is used in the *FireIgnitionRaw_Projections.py* script.

## FireIgnition_SummaryNormals.py
Script applies the High, Medium and Low Fire Ignition model classification by Land Cover Type to across defiend temporal ranges.  Subsequently processing summarize this classification acorss a defined temporal ranges which is defiend via the *HistoricCurrentProcessingList* table.  Summary periods are usually by normals periods (e.g.  Historic: 1991-2020, Futures 2031-2060, 2061-2090, etc.).
