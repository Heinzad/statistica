<!--docs\etl\extract\extract_spreadsheet_head.md-->



Extract Spreadsheet Head
========================

*Extract and unpivot hierachical headers of a pivot table.* 

Use the `extract_spreadsheet_head` method to extract question text embedded in the pivot table headers of a complex spreadsheet. 


Parameters
----------

The `extract_spreadsheet_head` method requires the names and file paths for the source and sink datasets. It requires further details on how many spreadsheet rows to skip to get to the beginning of the data table, and how many rows to extract from the pivot table headers. 

The `extract_spreadsheet_head` method also requires supplementary details about the survey for which the questions were recorded. 

```sheet_name : str```  
    Name of the spreadsheet to be extracted from an excel workbook. 

```file_path : str```  
    Absolute path to the Excel workbook containing the census results. 

```skiprows : int```  
    Number of rows to skip to beginning of data table. 

```nrows : int```  
    Number of rows to extract from data table headers. 

```survey : str```  
    The name of the survey - e.g. 'Census'. 

```dated : str```   
    The date of the survey - e.g. '2013'. 

```section : str```  
    The section of the survey being extracted - e.g. 'Individual part 1'. 

<br>

Returns
-------
pandas.DataFrame


Function
--------

```
# DEPENDENCIES

# Use pandas — 
import pandas


# INITIALISE 

# Let df be a pandas DataFrame — 
df = pandas.DataFrame()

# Let pvt be a pandas DataFrame — 
pvt = pandas.DataFrame()


# READ EXCEL WORKSHEET FROM FILE

# Let io be an Excel File from the given file_path — 
io = pandas.ExcelFile(file_path)

# Given sheet_name a string being the name of the sheet to extract from the excel file — 
sheet_name : str 

# Given skiprows an integer being the number of rows to skip from the top of the sheet — 
skiprows : int

# Let header be an integer as the row offset to use as a heading — 
header = None 

# Given nrows an integer being the number of rows to extract — 
nrows : int

# Let dtype be a string set to the object data type to import all values as text — 
dtype : str = 'object'

# Let engine be a string set to the excel engine to use. Set engine to openpyxl — 
engine : str = openpyxl

# read worksheet into pandas dataframe — 
df = pandas.read_excel(io=io, sheet_name=sheet_name, skiprows=skiprows, header=None, nrows=nrows, dtype=dtype, engine=engine)

# use openpyxl to replace dataframe column numbers with letters — 
for col in df.columns: 
    c = list(df.columns).index(col)
    alpha = get_column_letter( int(c) + 1 ) #avoid zero 
    df.rename(columns={col: alpha}, inplace = True)


# UNPIVOT HEADERS 

# Let pivot be a pandas dataframe to hold intermediate working
pvt0 = pandas.DataFrame()

# Reshape the dataframe from wide to long —  
pvt0 = df.transpose() 
pvt0.reset_index() 

# Rename columns —  
pvt0.columns = ['question_text', 'question_text2'] 

# Fill down on empty values — 
pvt0['question_text'] = pvt0['question_text'].ffill(axis=0) 

# Reindex — 
pvt0['question_code'] = pvt0.index  
pvt0.reset_index(drop=True, inplace=True)


# ADD SURVEY DETAILS

# Given survey a string for the name of the survey — 
survey : str

# Given dated a string for the date of the survey — 
dated : str

# Given section a string for the section or part of the survey — 
section : str

# set survey column values — 
pvt0['survey_name'] = survey
pvt0['survey_date'] = dated 
pvt0['survey_section'] = section

# reorder columns
pvt = pvt0.loc[:,['question_code', 'question_text', 'question_text2', 'survey_name', 'survey_date', 'survey_section']]


# RETURN

# return the final dataframe having columns in correct order
return pvt 

```


Example
-------

How to use the `extract_spreadsheet_head` method: 

```python
# import method from module 
from src.etl_extract import extract_spreadsheet_head

# initialise parameters 
sheet_name = '5 Regional Council Area' 
file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx"
skiprows = 8 
nrows = 2 
survey = 'Census' 
dated = '2013'
section = 'individual part 1'

# execute procedure
extract = extract_spreadsheet_head(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows, nrows=nrows,survey=survey, dated=dated, section=section) 

# inspect results
print(extract)
```

<br>
