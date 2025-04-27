<!--docs\etl\extract\get_access_db.md-->



Extract Access
==============

The `get_access_db` method reads from a Microsoft Access database and writes its tables into a sqlite database file. 


Parameters 
---------- 

The `get_access_db` method requires the file paths to the source and sink datasets: 

``` mdb_path : str ```  
- Absolute path to a Microsoft Access database file

```db_path : str ```  
- Absolute path to database file

<br>

Returns
-------
```Dict```  
    Key-Value dictionary of the number of rows inserted per table name 


Procedure  
--------- 

```
# DEPENDENCIES  

# Use pandas — 
from pandas import Dataframe, read_sql

# Use pyodbc for the source connection —   
from db_connect import connect_mdb 

# Use sqlalchemy for the sink connection — 
from etl.extract import put_dataframe

# PARAMETERS 

# Given mdb_path a string for the absolute path to the source db — 
mdb_path : str  

# Given db_path a string for the absolute path to the sink db —  
db_path : str  

# INITIALISE 

# Declare table_name a string for the name of the table to be extracted — 
table_name = str()

# Declare record_count an integer for the number records inserted in the data store for a given table 
record_count = int()

# Declare results a dictionary recording results of the number of rows inserted per table — 
results = dict() 

# CONNECT 

# Let mdb_conn be an Access database connection set from the given mdb_path string — 
mdb_conn = connect_mdb(mdb_path=mdb_path) 

# Let mdb_cursor be a cursor set from the Access database connection — 
mdb_cursor = mdb_conn.cursor()

# Let mdb_tables be a list set from the schema of the Access database — 
mdb_tables = mdb_cursor.tables(tableType='TABLE')

# LOOP   

# Loop through each table in the list — 
for tbl in mdb_tables: 

    # Set table_name from the table_name attribute — 
    table_name = tbl.table_name 

    # Let sql_statement be a formatted string of dynamic sql to select all rows from the source table — 
    sql_statement = f""" SELECT * FROM "{table_name}"; """

    # Let dataframe be a pandas dataframe to read the sql_statement to extract a table from the Access database — 
    dataframe = pandas.read_sql(sql_statement, mdb_conn)  

    # OUTPUT 

    # Store the dataframe by table_name using the given db_path — 
    row_count = put_dataframe(dataframe=dataframe, table_name=table_name, db_path=db_path) 

    # Record the result as number of rows inserted per table 
    results.update({table_name: row_count}) 

# RETURN

# Return rows inserted — 
return results
```

<br>

Example Usage
------------- 

How to use the `get_access_db` method: 

```python
# import method from module 
from src.etl_extract import get_access_db

# initialise parameters 
mdb_path = 'C:/Users/Public/Documents/CensusData.mdb'
db_path = 'C:/Users/Public/Documents/sample_db.sqlite'

# execute procedure
extract = get_access_db(mdb_path, db_path)

# inspect results
[print(f"""{k}: {v}""") for k,v in extract]
```

<br>


### Monitor Outcomes 

Verify the `get_access_db` method worked: 

```python
# libraries 
from sqlalchemy import text 

# modules 
from src.db_connect import connect_db

# connect to database 
conn = connect_db(db_path=db_path)

# sql statement for table names 
qry = text("""SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' """)

# execute the sql statement 
results = conn.execute(ods_qry)

# inspect results 
[print(tbl[0]) for tbl in results]
```
<br>

### Results 
 
```
tblCountsAreaUnit
tblCountsMeshBlock
tblCountsNewZealand
tblCountsRegionalCouncil
tblCountsTerritorialAuthority
tblCountsWard
tblGeogAreaUnit
tblGeogMeshBlock
tblGeogRegionalCouncil
tblGeogTerritorialAuthority
tblGeogWard
tblQuestions
tblSurveys
```
<br>

QED
