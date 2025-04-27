<!--docs\etl\extract\extract_spreadsheet_table.md-->



Extract Spreadsheet Table
========================= 

Use `extract_spreadsheet_table` to extract a data table with headers from a given spreadsheet. 

Parameters
----------

```sheet_name : str```   
    Name of the spreadsheet to be extracted from an excel workbook.  

```file_path : str```  
    Absolute path to the Excel workbook containing the census results.  

```skiprows : int```  
    Number of rows to skip to beginning of data table.  


Returns
-------

```DataFrame```


Function
--------

```
# DEPENDENCIES

# Use Pandas — 
import pandas


# INITIALISE

# Let df be a pandas DataFrame — 
df = pandas.DataFrame()


# READ EXCEL WORKSHEET FROM FILE

# Given file_path a string being the location where the workbook is stored — 
file_path : str 

# Let io be an Excel File at the given file_path — 
io = pandas.ExcelFile(file_path)

# Given sheet_name a string being the name of the worksheet to be extracted from the Excel file — 
sheet_name : str

# Given skiprows an integer being the number of rows to skip from the top of the sheet — 
skiprows : int

# Let header be an integer as the row offset to use as a heading. Set the header to the first row at the beginning of the data table — 
header : int = 0

# Let dtype be a string set to the object data type to import all values as text — 
dtype : str = 'object'

# Let engine be a string set to the excel engine to use. Set engine to openpyxl — 
engine : str = openpyxl

# Read Excel worksheet into pandas dataframe — 
df = pandas.read_excel(io=io, sheet_name=sheet_name, skiprows=skiprows, nrows=nrows, dtype=dtype, engine=engine)

# Delete empty or blank values
df.dropna(inplace = True)

# Use openpyxl to replace dataframe column numbers with letters — 
for col in df.columns: 
    c = list(df.columns).index(col)
    alpha = get_column_letter( int(c) + 1 ) #avoid zero 
    df.rename(columns={col: alpha}, inplace = True)


# RETURN

# return the dataframe 
return df 

```



Example 
------- 

Import the functionality from the `etl_extract` module: 

```python
# import method from module
from src.etl_extract import extract_spreadsheet_table

# initialise parameters
sheet_name = 'Footnotes and symbols'
file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx"
skiprows = 12
nrows = 14

# call the function
extract = get_spreadsheet_range(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows, nrows=nrows)
    
# inspect results
print(extract)

```