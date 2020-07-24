
"""
Text for the various parts of the grassland forecast website. These are in markdown
and are passed to dash markdown containers.
"""

about_tab_text = """
## Introduction
This tool is meant to answer the question "_How will climate change affect grasslands in the coming decades?_" It gives an estimate of the change in above-ground productivity (or forage) of grasslands in most of the United States along with estimates for the corresponding change in annual temperature and rain. 

## Usage
The map to the left represents areas where grassland forecasts are available. Click on any area with red shading, and an associated RCP tab to see the forecast for that location.   
In areas which are mostly forests, the estimates represent the amount of productivity for grassland patches such as meadows and pastures.  

## RCP What?
The amount of change in rain and temperature in any location due to climate change depends on how much greenhouse gas emissions are curtailed in the coming decades. The different RCP tabs represent those different scenarios. RCP 2.6 is an estimate of the best case scenario where global emissions are substantially reduced, while RCP 8.5 is the worst case scenario where emissions are not reduced at all.  
RCP stands for Representative Concentration Pathway and more can be read about them [here](https://en.wikipedia.org/wiki/Representative_Concentration_Pathway).

## ESA Conference 2020

Thanks for viewing my poster & site! This is in testing stage before being officially launched.   
Is there any other info you would like to see displayed? Do you feel this info would be useful to land-managers?  
Send feedback [here](https://forms.gle/WYSgf4WNeRQ3yfDX6), or ping Shawn on [twitter](https://twitter.com/dataEcologist).

## Technical Details
These forecasts use the Phenograss model derived in [Hufkins et al. 2016](https://doi.org/10.1038/nclimate2942), and optimized for grasslands in specific ecoregions. Climate data are from [downscaled CMIP5](https://gdo-dcp.ucllnl.org) projections. This interactive tool was build with [python dash](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwicyPnvkd_qAhXQVc0KHbL-A9IQFjAYegQIAhAB&url=https%3A%2F%2Fdash.plotly.com%2F&usg=AOvVaw0wNZrQX3xdcgdsG5iLfv2L) by [Shawn Taylor](https://twitter.com/dataEcologist) at the [USDA-ARS Jornada Experimental Range](https://jornada.nmsu.edu/).

"""


###############
# The descriptions of the different RCPs
rcp_tab_text = dict(rcp26 = '[RCP 2.6](https://en.wikipedia.org/wiki/Representative_Concentration_Pathway#RCP_2.6) is the best case scenario in climate mitigation. Here global emissions start declining by 2020 and go to zero by 2100.',
                    rcp45 = '[RCP 4.5](https://en.wikipedia.org/wiki/Representative_Concentration_Pathway#RCP_4.5) represents an intermediate scenario, where global emissions peak around 2040 and then begin to decline.',
                    rcp60 = '[RCP 6](https://en.wikipedia.org/wiki/Representative_Concentration_Pathway#RCP_6) is similar to RCP 4.5, but the peak emissions happen around 2080.',
                    rcp85 = '[RCP 8.5](https://en.wikipedia.org/wiki/Representative_Concentration_Pathway#RCP_8.5) is the worst case scenario, where global emissions never decline. Currently this scenario is seen as [unlikely](https://www.doi.org/10.1038/d41586-020-00177-3).')

