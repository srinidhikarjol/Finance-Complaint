from finance_complaint.config.pipeline.training import *
from finance_complaint.logger import logging
from finance_complaint.exception import FinanceException
from finance_complaint.config.spark_manager import spark_session
import os,sys
import pandas as pd
from finance_complaint.entity.metadata_entity import DataIngestionMetaData
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
#init ->config , n_retries
#get the required interval - from and to date
#download data
#data to parquet file
#retry download
#update metadata
DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig,n_retry: int = 5):

        logger.info(f"{'>>' * 20}Starting data ingestion.{'<<' * 20}")
        self.data_ingestion_config = data_ingestion_config
        self.n_retry = n_retry


    """
    To get the reqired interval based on whether is days,weeks or years
    """
    def get_required_interval(self) -> []:
        try:
            start_date = datetime.strptime(self.data_ingestion_config.from_date, "%Y-%m-%d")
            end_date = datetime.strptime(self.data_ingestion_config.to_date, "%Y-%m-%d")

            n_diff_days = (end_date - start_date).days

            freq = None #This freq will used as a parameter for pd.data_range method
            if n_diff_days > 365:
                freq = 'Y'
            elif n_diff_days > 30:
                freq = 'M'
            else:
                freq = 'W'   

            intervals_list = list()
            intervals_list = pd.date_range(
                start_date=start_date,end_date=end_date,freq=freq).astype('str').tolist()

            logger.debug(f"Prepared Interval: {intervals_list}") 

            if end_date not in intervals_list:
                intervals_list.append(end_date)

            return intervals_list         

        except Exception as e:
            raise FinanceException(e, sys)    

    """
    Loop through the intervals and download data
    """
    def download_files(self):
        try:
            #before you download the files, we will need the intervals
            #get_required_interval -> gets the required intervals
            required_intervals = self.get_required_interval()
            logger.info("Downloading files...")
            for index in range(1,len(required_intervals)):
                from_date,to_date = required_intervals[index-1],required_intervals[index]
                logger.debug(f"Generating data download url between {from_date} and {to_date}")
                datasource_url_download:str = self.data_ingestion_config.datasource_url
                url = datasource_url_download.replace("<todate>",to_date).replace("<fromdate>",from_date)
                logger.debug(f"Url: {url}")
                file_name = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_path = os.path.join(self.data_ingestion_config.download_dir,file_name)
                download_url_obj = DownloadUrl(url=url, file_path=file_path, n_retry=self.n_retry)
                self.download_data(download_url_obj)
        except Exception as e:
            raise FinanceException(e, sys)    

    """
    To download data for each indivisual interval
    """
    def download_data(self,download_data_obj:DownloadUrl):
        try:
            logger.info(f"Starting download operation - {download_data_obj.datasource_url}")
            download_dir = os.path.dirname(download_data_obj.file_path)

            #create the download directory
            os.makedirs(download_dir,exist_ok=True)

            #download data
            #Note - If download fails the failed response would be in data itself
            data = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})

            try:
                logger.info("Writing the downloaded files to json file")
                with open(download_data_obj.file_path,"w") as file_obj:
                    finance_complaint_data = list(map(lambda x: x["_source"],
                                                      filter(lambda x: "_source" in x.keys(),
                                                             json.loads(data.content)))
                                                  )

                    json.dump(finance_complaint_data, file_obj)
                logger.info(f"Downloaded data written to file path - {download_data_obj.file_path}")

            except Exception as e:
                # Since the download as failed, delete off the created file and call retry download method
                logger.info("Failed to download, hence retry again.")
                if os.path.exists(download_data_obj.file_path):
                    os.remove(download_data_obj.file_path)
                self.retry_download_data(data,download_data_obj)    

        except Exception as e:
            raise FinanceException(e,sys)     

    """
    Retry download, if the download fails
    """
    def retry_download_data(self,data,download_data_obj:DownloadUrl):
        try:
            if download_data_obj.n_retry == 0:
                self.failed_download_urls.append(download_url)
                logger.info(f"Unable to download file {download_data_obj.url}")
                return

            # Writing  response to understand why download failed
            failed_download_file_path = os.path.join(self.data_ingestion_config.failed_dir,
            os.path.basename(download_data_obj.file_path))   

            os.makedirs(failed_download_dir,exist_ok=True)

            with open(failed_download_file_path,"r") as file_obj:
                file_obj.write(data)

            # Create a new object but make sure to reduce the no of retries by 1
            download_data_obj = DownloadUrl(download_data_obj.url,
            download_data_obj.file_path,download_data_obj.n_retry-1)  

            # Again call the download data method 
            self.download_data(download_data_obj=download_data_obj)
        except Exception as e:
            raise FinanceException(e, sys)  

    """
    convert files to parquet
    """                     
    def convert_files_to_parquet(self) -> str:
        try:
            download_dir = self.data_ingestion_config.download_dir
            output_file_name = self.data_ingestion_config.file_name

            #create/exists feature store dir
            feature_store_dir = self.data_ingestion_config.feature_store_dir
            os.makedirs(feature_store_dir,exist_ok=True)

            file_path = os.path.join(download_dir,f"{output_file_name}")

            #loop through the files in the download dir(have a validation check)
            if not os.path.exists(download_dir):
                return file_path 

            logger.info(f"The parquet file will be created at - {file_path}")
            for file_name in os.listdir(download_dir):
                json_file_path = os.path.join(download_dir,file_name)
                logger.debug(f"Converting {json_file_path} into parquet format at {file_path}")
                df = spark_session.read.json(json_file_path)
                if df.count() > 0:
                    df.write.mode('append').parquet(file_path) 

            return file_path        
        except Exception as e:
            raise FinanceException(e, sys)        


    def update_meta_data(self,parquet_data_file_path:str):
        try:
            logger.info("Writing meta data info to meta file")
            metadata_obj = DataIngestionMetaData(metadata_file_path=self.data_ingestion_config.metadata_file_path)

            metadata_obj.write_metadata_info(from_date=self.data_ingestion_config.from_date,
                 to_date=self.data_ingestion_config.to_date, data_file_path=parquet_data_file_path)
            logger.info("Meta data file updated.")

        except Exception as e:
            raise FinanceException(e, sys)            


    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logger.info(f"Started downloading json file")
            if self.data_ingestion_config.from_date != self.data_ingestion_config.to_date:
                self.download_files()

            logger.info("Combining all the downloaded files to a parquet file")
            if  os.path.exists(self.data_ingestion_config.download_dir):
                file_path = self.convert_files_to_parquet()
                self.update_meta_data(parquet_data_file_path=file_path)

            feature_store_file_path = os.path.join(self.data_ingestion_config.feature_store_dir,
                                                   self.data_ingestion_config.file_name)    

            data_ingestion_artifact = DataIngestionArtifact(feature_store_file_path = feature_store_file_path,
             metadata_file_path = self.data_ingestion_config.metadata_file_path, download_dir=self.data_ingestion_config.download_dir)

            logger.info(f"Data ingestion is complete.")
            logger.info(f"Data Ingestion Artifact ->{data_ingestion_artifact}")

            return data_ingestion_artifact     
        except Exception as e:
            raise FinanceException(e, sys)      


def main():
    config = FinanceConfig()
    data_ingestion_config = config.get_data_ingestion_config()
    data_ingestion_obj = DataIngestion(data_ingestion_config=data_ingestion_config)  
    data_ingestion_artifact = data_ingestion_obj.initiate_data_ingestion()


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        logger.exception(e)