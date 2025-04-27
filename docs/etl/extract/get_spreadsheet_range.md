<!--docs\etl\extract\extract_spreadsheet_range.md-->



Extract Spreadsheet Range
========================= 

Use `extract_spreadsheet_range` to extract a range of cells without headers from census results given in spreadsheet format. 

Parameters
----------

```sheet_name : str```   
    Name of the spreadsheet to be extracted from an excel workbook. 
```file_path : str```  
    Absolute path to the Excel workbook containing the census results. 
```skiprows : int```  
    Number of rows to skip to beginning of data table. 
```nrows : int```  
    Number of rows to extract from data table headers.

Returns
-------

```pandas.DataFrame```


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

# Given nrows an integer being the number of rows to extract — 
nrows : int

# Let dtype be a string as the object data type to import all values as text — 
dtype : str = 'object'

# Let engine be a string as the excel engine to use. Set engine to openpyxl — 
engine : str = openpyxl

# read worksheet into pandas dataframe — 
df = pandas.read_excel(io=io, sheet_name=sheet_name, skiprows=skiprows, nrows=nrows, dtype=dtype, engine=engine)

# remove empty or blank values
df.dropna(inplace = True)

# use openpyxl to replace dataframe column numbers with letters — 
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
from src.etl_extract import extract_spreadsheet_range

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