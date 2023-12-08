# NYC-taxi_for_2014-2019

## Description

This project evaluates my ability to execute a mini project. The key tasks involve designing a star schema, setting up containers with terraform, transferring data with Azure Data Factory and creating notebooks in Databricks. The goal to ingest data into a medallion architecture (bronze, silver and gold containers), manipulate it through staging schema enforcement and create fact and dimensional tables for reporting in PowerBI.

## Files 

### Database Models 

1. **Conceptual Database Model:** 

   - Illustrates the basic star schema database layout.

2. **Logical Database Model:** 

   - Displays column names along with distributed primary and foreign keys.

3. **Physical Database Model:** 

   - Includes all elements from the logical database model, with added information on data types for each column. 


### Notebooks 

#### Bronze Notebook (Bronze_Completed_NYC.py) 

This notebook has the following steps:

1. Uploads data from `yellow_tripdata_2014` and `yellow_tripdata_2019` into separate DataFrames. 

2. Cleans up column titles. 

3. Drops unnecessary columns not pertinent to the end star schema. 

4. Adds requested columns: `file_name` (original file name) and `created_on` (creation timestamp). 

5. Saves the DataFrame in Delta format, partitioned by `vector_id`. 


#### Silver Notebook (Silver_Completed_NYC.py) 

This notebook focuses on schema cleanup and column reordering: 

1. Installs the `geopandas` package for working with the geojson file. 

2. Reads 2014 and 2019 files from the bronze container into two different DataFrames. 

3. Separates pickup/dropoff datetime columns into separate time and date columns. 

4. Utilizes a User-Defined Function (UDF) to generate a `location_id` from the geojson file using pickup/dropoff longitude and latitude. 

5. Applies conditions: `TotalAmount` should not be null and greater than 0, and `trip distance` should be greater than 0. 

6. Organizes DataFrames into a structure similar to the fact table and saves them into the silver container in Delta format, partitioned by `vector_id`. 


#### Gold Notebook (Gold_Completed_NYC.py) 
The final notebook where fact and dimensional tables are created: 

1. Initializes dimensional tables with allocated primary keys. 

2. Creates the fact table with the help of dimension tables. 

3. Converts tables into actual Delta tables.


#### Geojson File 

This file, provided through Databricks workspace, is crucial for processing longitude and latitude coordinates into integers. This aids in pinpointing locations on the map and obtaining location names for each allocated ID. 

## My Experience

I faced a couple of challenges during the project. The main hurdle was extracting the data from the original JSON file where it had multiple arrays and seperate data and meta roots. Also, loading the 2014 data to the Azure silver container took more than 2 hours without any job processing in Databricks. As a way to go about the long loading time of the data was that I had to leave the 2014 data and just work with the 2019 from the Silver container onwards. For the JSON file, I received a different version containing the same location_id and coordinates list. When it also came to loading the data via Azure database connection with Power BI the fact table will load till the last few rows and will stop loading. I was able to obtain the data through the catalog explorer.

## Overall Task
1. Review the conceptual, logical, and physical database models for an understanding of the project structure. 

2. Execute the Bronze Notebook (`Bronze_Completed_NYC.py`) to stage and clean the data. 

3. Proceed with the Silver Notebook (`Silver_Completed_NYC.py`) for schema cleanup and reordering. 

4. Use the Gold Notebook (`Gold_Completed_NYC.py`) for the creation of fact and dimensional tables. 

5. Ensure the Geojson file is available for processing location data.


## Images

This is the image of the three different databases shown below :

     - This is the Conceptual database : ![Conceptual Database Design completed](https://github.com/Artin00/NYC-taxi_for_2014-2019/assets/113461257/d39fc0a7-5b46-430e-b9ac-b42a94c401e7)

     - This is the Logical database : ![Logical Database Design complete](https://github.com/Artin00/NYC-taxi_for_2014-2019/assets/113461257/dfeb70ad-9c1a-4871-b7bd-f1637202a9bf)

     - This is the Physical database : ![Physical Database Design complete](https://github.com/Artin00/NYC-taxi_for_2014-2019/assets/113461257/df1ff198-0d1d-409c-b76d-1096f14e8abd)

These are the images of the Power BI file and the visuals that answer a-d

     - This is the visuals that show the answers for each questions, the ranking one is answered by the average tip earned by each vendor: ![Power BI task with visuals of questions a - d](https://github.com/Artin00/NYC-taxi_for_2014-2019/assets/113461257/a682faf8-b3eb-497e-a9d7-eebef2c5e2ec)

     - This is the relationships that have been connected within the Power BI desktop space: ![The relationship between the tables in Power BI](https://github.com/Artin00/NYC-taxi_for_2014-2019/assets/113461257/15b53d41-3efb-40cc-9712-b3d8e15fc5eb)


     

