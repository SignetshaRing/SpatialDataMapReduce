# Project 2 - Spatiotemporal Analysis with MapReduce

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-2.html

# Deliverables

The project specification defines several questions that you will answer with MapReduce jobs. You should edit this document (README.md) with your answers as you find them, including figures, references, etc. This will also serve as a way of tracking your progress through the milestones.

## Deliverable I

<h3>Warmup</h3>

><b><h4>Number of Records in the Dataset</h4></b><br>
The number of records in the entire nam dataset was found to be <b>323759744</b>

><b><h4>Geohashes that have snow throughout the year</h4></b><br>
Some Geohashes which had significant amount of snowcover throughout the year<br>
*fc2jr4sp5wh0*<br>
*c44nftm8uizz*<br>
*c43m2snbpzbp*<br>

As seen from the image, it wasn't surprising that some of these Geohashes were located on snow covered moutains.<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/Screen%20Shot%202017-11-13%20at%205.54.16%20PM.png" alt="Snow Cover" width="500" height="500">
><b><h4>Hottest Temperature in the dataset</h4></b><br>
The hottest temperature was found near Cancun in the Mexican state of **Quintana Roo at a high of 57.85 Celsius(331K)** which occured in the month of **August**.<br> *Record: d5dpds10m55b,331.39062*<br>
Although online validation couldn't be found for the abnormally high temperature in Cacncun, it can not be considered an anomaly as values close to 55 Celsius are known to occur in countries located along the equator of the Earth.

### Analysis
><h4><b> Geohashes where its high chances of being struck by lightning</h4></b><br>
*dqvu8,1.0*<br>
*dx6ry,1.0*<br>
*dmpnh,1.0*<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/geo_b_triangle.png" alt="Geohash B-Triangle" width="500" height="500"><br>
*Interestingly,* it was found that all of these locations were located near the Bermuda Triangle(!!!). This would confirm the intense lightning storms that are known to occur in the area, that have culminated in numerous disappearnces of flights in the area.<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/b_triangle.png" alt="Bermuda Triangle" width="500" height="500">

><b> <h4>Driest Month in the Bay Area</h4></b><br>
Driest month in the Bay area was found to be **March**. <br>
In order to determine the location of the Bay Area, http://geohash.gofreerange.com was used. The following locations were finalised: *"9q8y","9q8v","9q8u","9q8g","9q9n","9q9j","9q9h","9q95","9q97","9q9k","9q9m","9q9q"*<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/bay_area_geohash.png" alt="bay area geohash"><br>
<br>
>The precipitation histogram for the Bay Area looks as follows:<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/bay_area_humid.png" alt="bay area humidity"><br>

><b><h4>Travel Itinerary</h4></b><br>
<br>
The following locations were selected:<br>
9q8zhu -> Golden gate bridge<br>
dr5ru6 -> Empire State Building<br>
9w2m2v -> Grand canyon<br>
dk2yqv -> The bahamas<br>
9q5fh5 -> Griffith Observatory<br>

As can be seen the travellers in question are people who like the outdoors and admire good architecure. So it would be reasonable to assume that preferable climate conditions for them would mean an ambient temperature between 20 and 30 degree Celsius, and of course no rain fall to foil their plans. These parameters were taken into consideration while picking which of these locations can be visited during a particular month by the travellers. *Note: Fair bit of manual observation was involved in the process.*<br><br>
>The following Itenerary was built:<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/Travel_it.png" alt="Itenerary"><br>
As can been seen from the itenerary, The bahamas, and the Griffith Observatory can be visited any time of the year, whereas the other places should probably be avoided during the winter. Therefore a suitable plan for the travellers would be as follows:<br>
*Jan-Feb: Griffith Observatory,CA<br>
Mar-April: Grand Canyon, AZ<br>
May-July: Golden Gate Bridge, CA<br>
Aug-Oct: Empire State Buildind, NY<br>
Nov-Dec: The Bahamas<br>
*
><b><h4> Green Energy</h4></b><br>
The following Geohashes were found for a subset of the dataset:<br>
**Solar Farm Locations**<br>
Cloud: 9qjbwfvtjkxb 0.0<br>
Cloud: 9qn0463ep8h0 0.0<br>
Cloud: 9qn09vgek0eb 0.0<br>
**Wind Farm Locations**<br>
Wind: c81ctmd9mpkp 21.580797<br>
Wind: c81cd59207eb 21.3308<br>
Wind: c81cjen0n82p 21.2683<br>
**Combined Solar and Wind Farm Locations**<br>
dnxm01qr5jup<br>
Cloud: 0.0 Wind: 13.768299<br>
dnxjtrp8n1up<br>
Cloud: 0.5 Wind: 14.455799<br>
dnz883hf5d00<br>
Cloud: 0.5 Wind: 14.237049<br><br>

*Note:Presence of land, and soil porosity were also checked before finalizing on a particular location*<br>
The combined locations were found by taking a mean of the indices of the geohashes on both the Wind and Solar farm lists, and sorting the resulting lists and sorting the result.<br>
It was reassuring to see that some of the wind farm locations were along the central strip of the united states which is known to have high wind speeds as can be seen in this heat map.<br>
https://windexchange.energy.gov/maps-data/319
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/wind_farm.png" alt="windfarm"><br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/heat.png" alt="heatmap"><br>
>Also surprisingly when trying to locate one of the geohashes for the Solar farm, the following image popped up, which surely enough shows what look like solar panels right next to the predicted suitable location for a solar farm.(Talk about production value :P)
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/Screen%20Shot%202017-11-12%20at%207.24.28%20PM.png" alt="solar farm"><br>

><b><h4> Climate Chart</h4></b><br>
A climate chart for a particular Geohash can be seen here:<br>
The location chosen was the geohash with the hottest temperature as seen:
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/climate_overview.png" alt="climate overview"><br>

