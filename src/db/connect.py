#src\connect.py






import pyodbc 
from sqlalchemy import create_engine 


# MS ACCESS 

def connect_mdb(mdb_path:str): 
    """Returns a connection to a Microsoft Access database using pyodbc

    Parameters
    ----------
    mdb_path : str 
        Absolute path to MS Access database file 
    
    Example
    -------
    >>> mdb_path = 'C:/Users/Public/Documents/CensusData.mdb'
    >>> mdb_conn = connect_mdb(mdb_path=mdb_path)
    >>> mdb_cursor = mdb_conn.cursor()
    >>> mdb_tables = mdb_cursor.tables(tableType='TABLE')
    >>> [print(tbl.table_name) for tbl in mdb_tables]

    """
    connection_string = (
        """DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"""
        f"""DBQ={mdb_path};"""
    )
    connection = pyodbc.connect(connection_string)
    return connection 


# SQLITE 

def connect_db(db_path: str): 
    """"Returns a connection to a sqlite database using sqlalchemy 
    
    Parameters
    ----------
    db_path : str 
        Absolute path to database file

    Example 
    -------
    >>> from sqlalchemy import text
    >>> db_path = 'C:/Users/Public/Documents/test_db.sqlite'
    >>> db_conn = connect_db(db_path)
    >>> db_qry = text("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ")
    >>> results = db_conn.execute(db_qry)
    >>> [print(tbl[0]) for tbl in results] 
    """
    engine = create_engine(f"""sqlite:///{db_path}""") 
    connection = engine.connect()
    return connection 



