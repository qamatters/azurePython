from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
import yaml
from os.path import abspath


CONTAINER_NAME =  'deepakcontainer'
STORAGE_ACCOUNT = 'javacodevalidationdeepak'
STORAGEACCOUNTURL = "https://javacodevalidationdeepak.blob.core.windows.net"
constants = yaml.safe_load(open(abspath('constants.yml')))

def createBlobServiceClient():
      return  BlobServiceClient(
        account_url=STORAGEACCOUNTURL,
        credential= constants['storageaccountkey'])


def validateContainer(contianerName):
    blob_service_client = createBlobServiceClient()
    generator = blob_service_client.list_containers()
    for contianerNames in generator:
        if(contianerNames.name == contianerName) :
         print("\t container name: " + contianerNames.name + " exist int the list")
        else:
            print(f"Container {contianerName} does not exist")


def downloadBlob():
    blob_service_client_instance =createBlobServiceClient()
    blob_client_instance = blob_service_client_instance.get_blob_client(
        CONTAINER_NAME, 'AEM.txt', snapshot=None)
    blob_data = blob_client_instance.download_blob()
    data = blob_data.readall()
    print(data)

def donwloadParquetFile():
    blob_service_client_instance = createBlobServiceClient()
    blob_client_instance = blob_service_client_instance.get_blob_client(
    CONTAINER_NAME, 'userdata1.parquet', snapshot=None)
    blob_data = blob_client_instance.download_blob()
    stream = BytesIO()
    blob_data.readinto(stream)
    processed_df = pd.read_parquet(stream, engine='pyarrow')
    # print(processed_df.columns)
    return processed_df

def setDisplayForDataFrame(max_columns,max_rows, width):
    print("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    pd.set_option('display.max_columns', max_columns)
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.width', width)

def queryOnParquetFiles(query):
    processed_df = donwloadParquetFile()
    setDisplayForDataFrame(None,None,1000)
    # print(processed_df)
    # print (processed_df.head(3))
    print("---------------------Query Output------------------------------------------------------------------------------------------------------------------------------------------")
    print(processed_df.query(query))
    return processed_df.query(query)

def parquetFilesShowColumnDataFromTop(columnName, rows):
    processed_df = donwloadParquetFile()
    setDisplayForDataFrame(None,None,1000)
    print("---------------------Query Output------------------------------------------------------------------------------------------------------------------------------------------")
    print(processed_df[columnName].head(rows))

def parquetFilesShowColumnDataFromBottom(columnName, rows):
    processed_df = donwloadParquetFile()
    setDisplayForDataFrame(None,None,1000)
    print("---------------------Query Output------------------------------------------------------------------------------------------------------------------------------------------")
    print(processed_df[columnName].tail(rows))

def sorting(columnName,isAssending):
    processed_df = donwloadParquetFile()
    setDisplayForDataFrame(None,None,1000)
    print("---------------------Query Output------------------------------------------------------------------------------------------------------------------------------------------")
    print(processed_df.sort_values(by=[columnName],ascending=isAssending))

def showDataINHTML(df_diff):
    html = df_diff.to_html()
    # write html to file
    text_file = open("index.html", "w")
    text_file.write(html)
    text_file.close()
