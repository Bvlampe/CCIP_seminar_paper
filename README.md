# CCIP_seminar_paper
Data prepping for a PolSci paper analysing the relationship between conflict intensity and the post-conflict change in violent crime.

Uses various open-source datasets: from the UCDP/PRIO Armed Conflict Dataset series on conflict occurrence and intensity (https://ucdp.uu.se/downloads/), from the  World Bank on country population over the years (https://data.worldbank.org/indicator/SP.POP.TOTL), and from the UNODC on intentional homicide rates (https://dataunodc.un.org/dp-intentional-homicide-victims)

This data preparation code produces two alternative datasets, each with a different measurement of the independent variable (conflict intensity). One is based on the UCDP/PRIO Georeferenced Events Dataset, the other on the Battle-Related Deaths dataset. Only the former ended up being used in the analysis. The latter was created mostly for bughunting. In fact, the authors of the UCDP/PRIO Armed Conflict Dataset series explicitly discourage using the latter for geographical analysis. Therefore, the file used for the regression analysis is "output_GED.csv".
