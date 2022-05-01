import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import urllib
from containerHelper.container import setDisplayForDataFrame
import yaml
from os.path import abspath

def readSQLData():
    constants = yaml.safe_load(open(abspath('venv/constants.yml')))
    connection = pyodbc.connect('DRIVER=' + constants['driver'] + ';SERVER=tcp:' + constants['server'] + ';PORT=1433;DATABASE=' + constants['database'] + ';UID=' + constants['username'] + ';PWD=' + constants['password'])
    query = "SELECT * FROM [SalesLT].[Address];"
    setDisplayForDataFrame(None, None, 1000)
    df = pd.read_sql(query, connection)
    print(df)