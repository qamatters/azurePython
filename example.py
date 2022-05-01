from containerHelper.container import validateContainer, downloadBlob, donwloadParquetFile, queryOnParquetFiles, \
    showDataINHTML, parquetFilesShowColumnDataFromTop, parquetFilesShowColumnDataFromBottom, sorting
from database.sqlConnector import readSQLData
import pandas as pd

def run_sample():
    validateContainer('deepakcontainer')
    downloadBlob()
    donwloadParquetFile()
    pd1 = queryOnParquetFiles('first_name == "Amanda" and last_name == "Jordan"')
    pd2 =queryOnParquetFiles('first_name == "Amanda" and last_name == "Gray" or first_name == "Amanda" and last_name == "Jordan"')
    parquetFilesShowColumnDataFromBottom('country', 3)
    parquetFilesShowColumnDataFromTop('country', 10)
    sorting('country', False)

    print("difference is : ")

    df_diff = pd.concat([pd1, pd2]).drop_duplicates(keep=False)
    print(df_diff)

    showDataINHTML(df_diff)
    readSQLData()

if __name__ == '__main__':
    run_sample()
