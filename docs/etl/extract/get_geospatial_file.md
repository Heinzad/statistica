<!--docs\etl\extract\extract_geospatial_file.md-->
PROCEDURE


Extract Geospatial File
=======================

*Returns a GeoDataFrame with geometries converted to Well-Known-Text 
from a given geospatial file* 


Parameters
----------

```file_path : str```  
    Absolute path to the file


Returns
-------
geopandas.GeoDataFrame


Function 
-------- 
  
``` 
# Use geopandas —
import geopandas

# Given a file_path of string type —
file_path: str

# Let gdf be a geodataframe —
gdf = geopandas.GeoDataFrame()

# Read file into geodataframe for the given file_path — 
gdf = read_file(file_path) 

# Convert geometries into Well-Known-Text —
gdf = gdf.to_wkt() 

# output the geodataframe — 
return gdf
```


Example
-------

Given a zipped shapefile `AU1996` with a structure of 

```
    .zip
    ├── .shp — geometry
    ├── .shx — index 
    ├── .dbf — attributes
    ├── .prj — projection
    ├── .cpg — code page
    └── .xml — metadata
```

```python 
# import methods from module 
from src.etl.extract import extract_geospatial_file, put_result

# initialise parameters  
file_path = 'C:/Users/Public/Documents/AU1996.ZIP' 
dataframe = extract_geospatial_file(file_path=file_path, db_path=db_path, table_alias=table_alias) 
table_name = 'AU1996'
db_path = 'C:/Users/Public/Documents/test_db.sqlite' 

extract = put_result(dataframe=dataframe, table_name=table_name, db_path)

# inspect results 
print(extract)
```

<br>
