# Project 2 - Spatiotemporal Analysis with MapReduce

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-2.html

# Deliverables

The project specification defines several questions that you will answer with MapReduce jobs. You should edit this document (README.md) with your answers as you find them, including figures, references, etc. This will also serve as a way of tracking your progress through the milestones.

## Deliverable I

<h3>Warmup</h3>

><b>Number of Records in the Dataset</b><br>
The number of records in the entire nam dataset was found to be <b>323759744</b>

><b>Geohashes that have snow throughout the year</b><br>
Some Geohashes which had significant amount of snowcover throughout the year<br>
*fc2jr4sp5wh0*<br>
*c44nftm8uizz*<br>
*c43m2snbpzbp*<br>

As seen from the image, it wasn't surprising that some of these Geohashes were located on snow covered moutains.<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/Screen%20Shot%202017-11-13%20at%205.54.16%20PM.png" alt="Snow Cover" width="500" height="500">
><b>Hottest Temperature in the dataset</b><br>
The hottest temperature was found near Cancun in the Mexican state of **Quintana Roo at a high of 57.85 Celsius(331K)** which occured in the month of **August**.<br> *Record: d5dpds10m55b,331.39062*<br>
Although online validation couldn't be found for the abnormally high temperature in Cacncun, it can not be considered an anomaly as values close to 55 Celsius are known to occur in countries located along the equator of the Earth.

### Analysis
><b> Geohashes where its high chances of being struck by lightning</b><br>
*dqvu8,1.0*<br>
*dx6ry,1.0*<br>
*dmpnh,1.0*<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/geo_b_triangle.png" alt="Geohash B-Triangle" width="500" height="500"><br>
*Interestingly,* it was found that all of these locations were located near the Bermuda Triangle(!!!). This would confirm the intense lightning storms that are known to occur in the area, that have culminated in numerous disappearnces of flights in the area.<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/b_triangle.png" alt="Bermuda Triangle" width="500" height="500">

><b> Driest Month in the Bay Area</b><br>
Driest month in the Bay area was found to be **March**. <br>
In order to determine the location of the Bay Area, http://geohash.gofreerange.com was used. The following locations were finalised: *"9q8y","9q8v","9q8u","9q8g","9q9n","9q9j","9q9h","9q95","9q97","9q9k","9q9m","9q9q"*<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/bay_area_geohash.png" alt="bay area geohash"><br>
<br>
>The precipitation histogram for the Bay Area looks as follows:<br>
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/bay_area_humid.png" alt="bay area humidity"><br>

><b> Travel Itinerary </b><br>

><b> Green Energy</b><br>


><b> Climate Chart</b><br>
A climate chart for a particular Geohash can be seen here:<br>
The location chosen was the geohash with the hottest temperature as seen above:
<img src="https://github.com/cs686-bigdata/p2-cmattey/blob/master/images/climate_overview.png" alt="climate overview"><br>

