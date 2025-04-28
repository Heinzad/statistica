#src\etl\extract.py

""" 

Extract Module  
============== 

Extracts source data into a data store. 


Methods 
------- 

ingest_geospatial_file(file_path:str, db_path:str, table_name:str): 
    Read a given geospatial file and write to a given data store. Returns nothing. 

ingest_spreadsheet_table(sheet_name:str, file_path:str, skiprows:int, db_path:str, table_name:str): 
    Read a data table in a given spreadsheet and write to a given data store. Returns nothing. 

ingest_spreadsheet_range(sheet_name:str, file_path:str, skiprows:int, nrows:int, db_path:str, table_name:str): 
    Read a range of cells in a given spreadsheet and write to a given data store. Returns nothing.

ingest_spreadsheet_body(sheet_name:str, file_path:str, skiprows:int, db_path:str, table_name:str): 
    Read the body of a pivot table with hierachical headers in a given spreadsheet and write to a given data store. Returns nothing.

ingest_spreadsheet_head(sheet_name:str, file_path:str, skiprows:int, nrows:int, survey:str, dated:str, section:str, db_path:str, table_name:str='Questions'): 
    Read the body of a pivot table with hierachical headers in a given spreadsheet and write to a given data store. Returns nothing.

ingest_access_db : 
    Extract Microsoft Access database tables into a sqlite database —  
    Returns number of database records created.

_put_dataframe : 
    Store dataframes in a sqlite database — 
    Returns number of records created in database for a given dataframe, table name, and database path

_get_geospatial_file : 
    Extract a geospatial file — 
    Returns a geopandas geodataframe 

_get_spreadsheet_table : 
    Extract a data table that has headers — 
    Returns a pandas dataframe

_get_spreadsheet_range : 
    Extract a range of cells without headers — 
    Returns a pandas dataframe 

_get_spreadsheet_body : 
    Extract and unpivot the body of a pivot table — 
    Returns a tuple of pandas dataframes 

_get_spreadsheet_head : 
    Extract and unpivot hierachical headers of a pivot table — 
    Returns a pandas dataframe 

"""

import pandas as pd
import geopandas as gpd
from openpyxl.utils import get_column_letter 
from typing import Tuple, Dict
from sqlalchemy.types import NVARCHAR
from src.db.connect import connect_mdb, connect_db 


    

def ingest_geospatial_file(file_path:str, db_path:str, table_name:str): 
    """Read a given geospatial file and write to a given data store. Returns nothing. """
    shp = _get_geospatial_file(file_path=file_path)
    shp.pipe(_put_dataframe, db_path=db_path, table_name=table_name)



def ingest_spreadsheet_table(sheet_name:str, file_path:str, skiprows:int, db_path:str, table_name:str): 
    """Read a data table in a given spreadsheet and write to a given data store. Returns nothing. """
    tbl = _get_spreadsheet_table(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows)
    tbl.pipe(_put_dataframe, db_path=db_path, table_name=table_name)



def ingest_spreadsheet_range(sheet_name:str, file_path:str, skiprows:int, nrows:int, db_path:str, table_name:str): 
    """Read a range of cells in a given spreadsheet and write to a given data store. Returns nothing. """
    rng = _get_spreadsheet_range(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows, nrows=nrows)
    rng.pipe(_put_dataframe, db_path=db_path, table_name=table_name)



def ingest_spreadsheet_body(sheet_name:str, file_path:str, skiprows:int, db_path:str, table_name:str): 
    """Read the body of a pivot table with hierachical headers in a given spreadsheet and write to a given data store. Returns nothing. """
    body = _get_spreadsheet_body(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows, table_name=table_name)
    counts = body[0]
    counts.pipe(_put_dataframe, db_path=db_path, table_name=table_name)
    tracts = body[1]
    tracts.pipe(_put_dataframe, db_path=db_path, table_name=table_name)



def ingest_spreadsheet_head(sheet_name:str, file_path:str, skiprows:int, nrows:int, survey:str, dated:str, section:str, db_path:str, table_name:str='Questions'): 
    """Read the body of a pivot table with hierachical headers in a given spreadsheet and write to a given data store. Returns nothing. """
    head = _get_spreadsheet_head(sheet_name=sheet_name, file_path=file_path, skiprows=skiprows, table_name=table_name)
    head.pipe(_put_dataframe, db_path=db_path, table_name=table_name)



def ingest_access_db(mdb_path:str, db_path:str)->Dict: 
    """Read Microsoft Access database tables and write into a sqlite database — 
    Returns dictionary of number of rows inserted per table. 
    
    Parameters
    ----------
    mdb_path : str 
        Absolute path to a Microsoft Access database file
    db_path : str 
        Absolute path to database file 
    
    Returns
    -------
    dict 
        { table name : row count }
    
    Example
    -------
    >>> extract = store_access_db(
    ... mdb_path = 'C:/Users/Public/Documents/CensusData.mdb',
    ... db_path = 'C:/Users/Public/Documents/test_db.sqlite'
    ... )
    >>> [print(k,v) for k,v in extract]
    
    """ 
    table_name = str()
    row_count = int()
    results = dict()
    mdb_conn = connect_mdb(mdb_path=mdb_path)
    mdb_cursor = mdb_conn.cursor()
    mdb_tables = mdb_cursor.tables(tableType='TABLE')
    for tbl in mdb_tables: 
        row_count = 0 
        table_name = tbl.table_name 
        dataframe = pd.read_sql(f"""SELECT * FROM "{table_name}";""", mdb_conn)
        row_count = _put_dataframe(dataframe=dataframe, table_name=table_name, db_path=db_path)
        results.update({table_name: row_count})
    return results


# helpers 



def _put_dataframe(dataframe: pd.DataFrame, table_name:str, db_path:str) -> (int | None): 
    """Store dataframes in a sqlite database — 
    Returns number of records created on saving a given DataFrame to a database table with a given name. 
    
    Parameters
    ----------
    dataframe : pandas.DataFrame
        The DataFrame to be stored as a database table. 
    table_name : str
        Name of the table to be created. 
    db_path : str 
        Absolute path to database file 

    Returns
    -------
    int or None
        Number of rows affected by to_sql (rowcount), otherwise None.  
    
    Example
    -------
    >>> file_path = 'C:/Users/Public/Documents/AU1996.ZIP'
    >>> dataframe = _get_geospatial_file(file_path=file_path, db_path=db_path, table_alias=table_alias)
    >>> table_name = 'AU1996'
    >>> db_path = 'C:/Users/Public/Documents/test_db.sqlite'
    >>> result = _put_dataframe(dataframe=dataframe, table_name=table_name, db_path)
    >>> print(result)

    """ 
    db_conn = connect_db(db_path=db_path)
    return dataframe.to_sql(
        name= table_name,
        con=db_conn,
        schema=None, # default schema
        if_exists='replace', 
        index_label='_id', 
        chunksize=1048576, 
        dtype=NVARCHAR # import all values as text
    ) 



def _get_geospatial_file(file_path:str) -> gpd.GeoDataFrame: 
    """Extract a geospatial file — 
    Returns a GeoDataFrame with geometries converted to Well-Known-Text 
    from a given geospatial file. 
    
    Parameters
    ----------
    file_path : str
        Absolute path to the file

    Returns
    -------
    geopandas.GeoDataFrame 

    Example
    -------
    >>> extract = _get_geospatial_file(file_path = 'C:/Users/Public/Documents/AU1996.ZIP') 
    >>> print(extract)
    
    """
    src = gpd.read_file(file_path)  
    stg = src.copy() 
    return stg.to_wkt() 


    
def _get_spreadsheet_table(sheet_name:str, file_path:str, skiprows:int) -> pd.DataFrame: 
    """Extract a data table that has headers — 
    Returns a pandas DataFrame for a data table in a given excel workbook
    
    Parameters
    ----------
    sheet_name : str 
        Name of the spreadsheet to be extracted from an excel workbook. 
    file_path : str
        Absolute path to the Excel workbook containing the census results. 
    skiprows : int
        Number of rows to skip to beginning of data table. 

    Returns
    -------
    pandas.DataFrame

    Example
    ------- 
    >>> extract = _get_spreadsheet_table(
    ... sheet_name = 'Geographic Key',
    ... file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx",
    ... skiprows = 2
    ... )
    >>> print(extract)
    
    """ 
    src = pd.read_excel(
        io = pd.ExcelFile(file_path), 
        sheet_name=sheet_name, 
        skiprows=skiprows, 
        header=0, 
        dtype='object', 
        engine='openpyxl'
    )
    stg = src.copy() 
    stg.dropna(inplace = True) 
    return stg
    


def _get_spreadsheet_range(sheet_name:str, file_path:str, skiprows:int, nrows:int) -> pd.DataFrame: 
    """Extract a range of cells without headers — 
    Returns a pandas DataFrame for a range in a given excel workbook
    
    Parameters
    ----------
    sheet_name : str 
        Name of the spreadsheet to be extracted from an excel workbook. 
    file_path : str
        Absolute path to the Excel workbook containing the census results. 
    skiprows : int
        Number of rows to skip to beginning of data table. 
    nrows : int
        Number of rows to extract from data table headers.

    Returns
    -------
    pandas.DataFrame

    Example
    ------- 
    >>> extract = _get_spreadsheet_range(
    ... sheet_name = 'Footnotes and symbols',
    ... file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx",
    ... skiprows = 12,
    ... nrows = 14
    ... )
    >>> print(extract)
    
    """ 
    src = pd.read_excel(
        io = pd.ExcelFile(file_path), 
        sheet_name=sheet_name, 
        skiprows=skiprows, 
        nrows=nrows, 
        dtype='object', 
        engine='openpyxl'
    )
    stg = src.copy() 
    stg.dropna(inplace = True) 
    for col in stg.columns: 
        c = list(stg.columns).index(col)
        alpha = get_column_letter( int(c) + 1 ) #avoid zero 
        stg.rename(columns={col: alpha}, inplace = True) 
    return stg 



def _get_spreadsheet_body(sheet_name:str, file_path:str, skiprows:int, table_name:str ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract and unpivot the body of a pivot table into a long dataframe — 
    Returns a tuple of pandas DataFrames for geographies and counts respectively 
    from the body of a pivot table in a given excel workbook
    
    Parameters
    ----------
    sheet_name : str 
        Name of the spreadsheet to be extracted from an excel workbook. 
    file_path : str
        Absolute path to the Excel workbook containing the census results. 
    skiprows : int
        Number of rows to skip to beginning of data table. 
    table_name : str 
        Name to be used in database table. 
    
    Returns
    ------- 
    Tuple[pandas.DataFrame, pandas.DataFrame]
        A tuple of pandas dataframes for extracted geographies and counts respectively
    
    Example
    -------
    >>> extract = _get_spreadsheet_body(
    ... sheet_name = '1 Meshblock',
    ... file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx",
    ... skiprows = 10,
    ... table_name = 'MeshBlock'
    ... )
    >>> print(extract[0])
    >>> print(extract[1])

    """
    table_name = table_name.lower
    src = pd.read_excel(
        io = pd.ExcelFile(file_path), 
        sheet_name=sheet_name, 
        skiprows=skiprows, 
        header=None,
        dtype='object', 
        engine='openpyxl'
    ) 
    stg = src.copy() 
    for col in stg.columns: 
        c = list(stg.columns).index(col)
        alpha = get_column_letter( int(c) + 1 ) 
        stg.rename(columns={col: alpha}, inplace = True) 
    stg['A'] = stg['A'].fillna('00') 
    if table_name == 'meshblock': 
        dfg = stg[['A']] 
        dfg.columns = [f'{table_name}_code'] 
        dfc0 = stg 
    else: 
        dfg = stg[['A', 'B']] 
        dfg.columns = [f'{table_name}_code', f'{table_name}_description'] 
        dfc0 = stg.drop(['B'], axis=1) 
    dfc1 = dfc0.set_index(['A']).stack() 
    dfc2 = dfc1.to_frame() 
    dfc = dfc2.reset_index() 
    dfc.columns = [f'{table_name}_code', 'question_code', 'response_count'] 
    return dfc, dfg  



def _get_spreadsheet_head(sheet_name:str, file_path:str, skiprows:int, nrows:int, survey:str, dated:str, section:str, table_name:str='Questions'): 
    """Extract and unpivot hierarchical headers of a pivot table — 
    Returns a pandas dataframe from unpivoted multi-line headings in the head of a pivot table in a 
    given excel workbook. 
    
    Parameters
    ----------
    sheet_name : str 
        Name of the spreadsheet to be extracted from an excel workbook. 
    file_path : str
        Absolute path to the Excel workbook containing the census results. 
    skiprows : int
        Number of rows to skip to beginning of data table. 
    nrows : int
        Number of rows to extract from data table headers. 
    survey : str
        The name of the survey - e.g. 'Census'. 
    dated : str 
        The date of the survey - e.g. '2013'.
    section : str
        The section of the survey being extracted - e.g. 'Individual part 1' 
    
    Returns
    -------
    pandas.DataFrame

    Example
    -------
    >>> extract = extract_spreadsheet_head(
    ... sheet_name = '5 Regional Council Area',
    ... file_path = "C:/Users/Public/Documents/2013-mb-dataset-Total-New-Zealand-individual-part-1.xlsx",
    ... skiprows = 8,
    ... nrows = 2,
    ... survey = 'Census',
    ... dated = '2013',
    ... section = 'individual part 1'
    ... )
    >>> print(extract)
    """
    src = pd.read_excel(
        io = pd.ExcelFile(file_path), 
        sheet_name=sheet_name, 
        skiprows=skiprows,
        header=None,
        nrows=nrows,
        dtype='object',
        engine='openpyxl'
    )
    stg = src.copy() 
    for col in stg.columns: 
        c = list(stg.columns).index(col)
        alpha = get_column_letter( int(c) + 1 )
        stg.rename(columns={col: alpha}, inplace = True) 
    pvt0 = stg.transpose() 
    pvt0.reset_index() 
    pvt0.columns = ['question_text', 'question_text2']  
    pvt0['question_text'] = pvt0['question_text'].ffill(axis=0) 
    pvt0['question_code'] = pvt0.index 
    pvt0.reset_index(drop=True, inplace=True)
    pvt0['survey_name'] = survey
    pvt0['survey_date'] = dated 
    pvt0['survey_section'] = section
    pvt = pvt0.loc[:,['question_code', 'question_text', 'question_text2', 'survey_name', 'survey_date', 'survey_section']]
    return pvt 
    
