# Movies ETL

## Project Overview
Amazing Prime, an online movie streaming service is trying to improve their proft margins, and would like to be able to predict which low-budget movies will be popular with their customers. Amazing Prime would like to develop an algorithm to facilitate this, so they are holding a hackathon!

In order to get information for the hackathon, they are pulling their information from two sources: Wikipedia scrape for movies, and Rating data from a kaggle dataset that pulls information from the MovieLens dataset.


## Resources
- Data source: wikipedia.movies.json, ratings.csv from kaggle dataset
- Software: PostgreSQL 11.6, Visual Studio Code 1.40.2
- Python libraries: JSON, Pandas, NumPy, Re, psycopg2, sqlalchemy, time, config

Please note that ratings.csv and kaggle_metadata.csv files could not be uploaded to GitHub, due to the files being too large. 

## Movies ETL Objectives
The Extract, Transform, Load (ETL) process to create data pipelines. A data pipeline moves data from a source to a destination, and the ETL process creates data pipelines that also transform the data along the way. Analysis is impossible without access to good data, so creating data pipelines is often the first step before any analysis can be performed. Therefore, understanding ETL is an essential skill for data analysis.

### Extract
Data is pulled from different sources, in this case Wikipedia as a JSON file and Kaggle data stored as a CSV. The extracted data is then held in a staging area in between the data sources and data targets. 

### Transform
The transformation phase can be accomplished with Python and Pandas, pure SQL, or specialized ETL tools. We will use Python and Pandas to explore, document, and perform our data transformation. This process can have many iterations, and be performed over and over until the data is finally clean. 

### Load
Finally, after the data is transformed into a consistent structure, it’s loaded into the data target. 
For the hackathon is was decided that a SQL database is the best solution for sharing the data, so we’ll be loading our data into a PostgreSQL table.

## Challenge Objective
Amazing Prime loves the dataset and wants to keep it updated on a daily basis. They need to create an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables.

## Challenge Summary
The final code from the [module](https://github.com/hillarykrumbholz/Movies_ETL/blob/master/Movies_ETL.ipynb) was used to write a Python script that performs all three ETL steps on the Wikipedia and Kaggle data. It is preferred to leave out any code that performs exploratory data analysis.

The final, cleaned code can be seen in either [Challenge python](https://github.com/hillarykrumbholz/Movies_ETL/blob/master/Challenge.py) file, or in the [Challenge JupyterNB](https://github.com/hillarykrumbholz/Movies_ETL/blob/master/Challenge.ipynb) file. 

There were assumptions made in order to successfuly clean the data. They are as follows:
1. The data sources only came from two sources and do not change. If there was a change or if an additional file is needed for data, then the read and load scripts would need to be altered. 
2. When looking at and comparing data from Wikipedia and Kaggle, it appeared that the data between the two did not always match. In a few cases such as title, Kaggle had a more consistent structure. So we confirmed there were not any missing values in the Kaggle data, and dropped Wikipedia data. In other cases such as budget, we kept Kaggle data, but filled in any zeros it had with data from Wikipedia. 
3. Column names between Kaggle and Wikipedia differed, so a large part of the data cleaning process was to delete columns, without deleting needed data. Try-Else statements were used to make sure the code continued to run in the case errors occured. 
4. We assumed that when merging the two data sources the common column name, imdb_id would not change. If this were to change then our left-join would not work. 
5. The local database in PostgreSQL would not change, meaning that the owner and server of this new large dataset would continue to be handled by one individual, which is unlikey in a company like Amazing Prime. 
