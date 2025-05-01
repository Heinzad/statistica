statistica



Census Statistics 
================= 

This project builds a data model and etl processes for loading data from Statistics NZ (statsnz). 
At present, census data has been made accessible online in csv or excel file formats. 
Previously, census data was supplied in an MS Access database file with a simple star schema, supplemented with geospatial files. 
This project extends that schema to cover more recent censuses and other related datasets from Statistics NZ. 


Approach 
-------- 

The general approach is to extract the source data for each census into a standalone SQLite database, treating each dataset on its own merits. 
The source data is then transformed into dimension and fact tables. 
New or updated data can then be loaded into a unified data model in Postgres. 


Structure 
--------- 

The project is structured into the following directories: 

```
    statistica
    ├── .venv/
    ├── docs/
    ├── samples/
    ├── src/
    |   ├── etl/
    |   └── main.py
    ├── tests/
    ├── .env
    ├── .gitignore
    ├── LICENSE
    └── README.md
``` 

The folders contain: 

- docs: documentation 
- samples: example scripts 
- src: source code
- base: database schematics
- tests: test scripts
- .venv: virtual environment 


Environment Variables
---------------------

Set environment variables in the `.env` file: 

```
# Logging Directory 
LOGDIR=
```


Dependencies 
------------ 

Installing the principal packages listed here will also install a number of dependencies that are reflected in the `requirements.txt` file. 

| Package Name | Remarks | 
| ------------ | ------- | 
| Pandas | Python Data Analysis Library | 
| GeoPandas | Geospatial extension to Pandas | 
| pyodbc | Connects to Microsoft Access Databases |
| SQL Alchemy | Connects to SQLite, Postgres, Oracle databases, etc | 
| openpyxl | Connects to Excel files | 

