<!--docs\etl_extract\overview.md-->



Extract Spreadsheet Body
========================

*Extract and unpivot the body of a pivot table into a long dataframe*

Use the `get_spreadsheet_body` method to extract counts and geographies from the body of a complex spreadsheet. 


Parameters
----------

The `get_spreadsheet_body` method requires the names and file paths for the source and sink datasets, plus how many spreadsheet rows to skip to get to the beginning of the data table. 

```sheet_name : str``` 
- Name of the spreadsheet to be extracted from an excel workbook. 

```db_path : str```
- Absolute path to database file that will collate the results. 

```file_path : str```
- Absolute path to the Excel workbook containing the census results. 

```skiprows : int```
- Number of rows to skip to beginning of data table. 

```table_name : str``` 
- Name to be used when storing results as a database table.

<br>

Function
--------

```
# DEPENDENCIES

# Use pandas — 
from pandas import Dataframe, ExcelFile, read_excel

# Use openpyxl —   
from openpyxl import get column_letter 

# PARAMETERS

# Given sheet_name a string being the name of the worksheet to extract from an Excel workbook —  
sheet_name : str

# Given file_path a string being the absolute path to the workbook from containing the worksheet to be extracted —  
file_path : str

# Given skip_rows an integer being the number of rows to skip to the beginning of the body of a pivot table — 
skip_rows : int

# Given table_name a string being the name to be used for the resulting database table — 
table_name : str 

# INITIALISE 

# set table_name to lower case for consistency
table_name.lower

# Declare df to be a pandas DataFrame
df = pandas.DataFrame() 

# Declare dfg a pandas dataframe to hold geographic areas — 
dfg = pandas.DataFrame() 

# Declare dfc a pandas dataframe to hold response counts — 
dfc = pandas.DataFrame() 


# READ EXCEL 

# Let io be set from a pandas excel file for the given file_path — 
io = pandas.ExcelFile(file_path)

# Let header be set to nothing as no header is being used — 
header = None

# Let dtype be set to an object to import all values as text — 
dtype = 'object'

# Let engine be set to openpyxl to work with the excel file
engine = 'openpyxl' 

# read excel worksheet into pandas dataframe —  
df = pd.read_excel(io = io, sheet_name=sheet_name, skiprows=skiprows, header=header, dtype=dtype, engine=engine)

# COLUMN LABELLING 

# use openpyxl to replace dataframe column numbers with letters —   
for col in df.columns: 
    c = list(df.columns).index(col)
    alpha = openpyxl.get_column_letter( int(c) + 1 ) #avoid zero 
    df.rename(columns={col: alpha}, inplace = True) 

# Update dataframe to replace empty key values with zero to represent New Zealand — 
df['A'] = df['A'].fillna('00') 


# IF MESHBLOCK THEN NO DESCRIPTION COLUMN

# If the given table_name is 'Meshblock' —  
if table_name. == 'meshblock' :

    # Set geographic dataframe to geography code column from df
    dfg = df[['A']] 

    # rename geography code column — 
    dfg.columns = [f'{table_name}_code'] 

    # Set counts dataframe to all columns from df 
    dfc0 = df 
    
else: 

    # Otherwise set geographic datagrame to geography code and description — 
    dfg = df[['A', 'B']] 
    
    # rename geography code and geography description columns —  
    dfg.columns = [f'{table_name}_code', f'{table_name}_description']

    # set counts dataframe to all but the geography description column — 
    dfc0 = df.drop(['B'], axis=1) 


# RESHAPE COUNTS DATAFRAME

# 1. reshape counts dataframe from wide to long on 'geog key' and 'question key' — 
dfc1 = dfc0.set_index('A').stack() 

# 2. convert counts series to dataframe — 
dfc2 = dfc1.to_frame() 

# 3. convert multi-index into column — 
dfc = dfc2.reset_index() 

# 4. rename columns — 
dfc.columns = [f'{table_name}_code', 'question_code', 'response_count'] 


# RETURN 

# return a tuple of counts and geographies dataframes —  
return dfc, dfg 

```

<br>

Example
-------

How to use the `get_spreadsheet_body` method:

```python 
# import method from module 
from src.etl.extract import get_spreadsheet_body

# initialise parameters  
sheet_name = '1 Meshblock' 
file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx"
skiprows = 10 
table_name = 'MeshBlock' 

# execute procedure 
extract = get_spreadsheet_body(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows, table_name=table_name) 

# inspect results 
print(extract[0])
print(extract[1])
```

<br>


Inspect Results
---------------

How to verify the `get_spreadsheet_body` method worked: 

```python
# libraries 
from sqlalchemy import text 

# modules 
from src.db_connect import connect_db

# connect to database 
conn = connect_db(db_path=db_path)

# sql statement for table names 
qry = text("SELECT name FROM sqlite_schema WHERE type = 'table' AND lower(name) LIKE '%meshblock' ")

# execute the sql statement 
results = conn.execute(ods_qry)

# inspect results 
[print(tbl[0]) for tbl in results]
```

<br>


### Results 
 
```
tblGeogMeshBlock
tblCountsMeshBlock
```
<br>

... 
