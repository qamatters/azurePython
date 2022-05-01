from containerHelper.container import validateContainer, downloadBlob, donwloadParquetFile, queryOnParquetFiles, \
    showDataINHTML, parquetFilesShowColumnDataFromTop, parquetFilesShowColumnDataFromBottom, sorting
from database.sqlConnector import readSQLData
import pandas as pd

def run_sample():
    filename = "userdata1.parquet"
    validateContainer('deepakcontainer')
    downloadBlob()
    donwloadParquetFile(filename)
    pd1 = queryOnParquetFiles('first_name == "Amanda" and last_name == "Jordan"', filename)
    pd2 =queryOnParquetFiles('first_name == "Amanda" and last_name == "Gray" or first_name == "Amanda" and last_name == "Jordan"', filename)
    parquetFilesShowColumnDataFromBottom('country', 3, filename)
    parquetFilesShowColumnDataFromTop('country', 10, filename)
    sorting('country', False, filename)

    print("difference is : ")

    df_diff = pd.concat([pd1, pd2]).drop_duplicates(keep=False)
    print(df_diff)

    showDataINHTML(df_diff)
    readSQLData()

if __name__ == '__main__':
    run_sample()
