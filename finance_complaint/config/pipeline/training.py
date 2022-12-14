from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.constant.training_pipeline_config.data_ingestion_config import *
from finance_complaint.constant import TIMESTAMP
from finance_complaint.constant.training_pipeline_config.data_ingestion_config import DATA_INGESTION_MIN_START_DATE
from finance_complaint.entity.config_entity import TrainingPipelineConfig,DataIngestionConfig
import os,sys
from time import strftime
from datetime import datetime
from finance_complaint.entity.metadata_entity import DataIngestionMetaData

class FinanceConfig:

    def __init__(self,timestamp = TIMESTAMP, pipeline_name = PIPELINE_NAME):
        """
        Initialize the required values 
        """
        self.timestamp = timestamp
        self.pipeline_name = pipeline_name
        self.pipeline_config = self.get_pipeline_config()

    def get_pipeline_config(self):
        """
        To get the pipeline config
        """
        try:
            artifact_dir = PIPELINE_ARTIFACT_DIR
            training_pipeline_config = TrainingPipelineConfig(pipeline_name=self.pipeline_name, artifact_dir=artifact_dir)
            return training_pipeline_config
        except Exception as e:
            raise FinanceException(e, sys)

    def get_data_ingestion_config(self,from_date=DATA_INGESTION_MIN_START_DATE,to_date=None):
        """
        To get the data ingestion config object -> DataIngestionConfig
        """
        try:
            if to_date is None:
                to_date = datetime().now().strftime("%Y-%m-%d")

            from_date = datetime().strftime(from_date,"%Y-%m-%d")    
            min_from_date = datetime().strftime(DATA_INGESTION_MIN_START_DATE,"%Y-%m-%d")

            if from_date < min_from_date:
                from_date = DATA_INGESTION_MIN_START_DATE

            # Data ingestion main directory
            data_ingestion_master_dir = os.path.join(self.pipeline_config.artifact_dir,DATA_INGESTION_DIR)  
            # timestamp after every run
            data_ingestion_dir = os.path.join(data_ingestion_master_dir,self.timestamp)
            metadata_file_path = os.path.join(data_ingestion_master_dir,DATA_INGESTION_METADATA_FILE_NAME)  

            #Before we get the data, lets check when the data was fetched last - from the meta data
            metadata = DataIngestionMetaData(metadata_file_path=metadata_file_path)

            #Check if metadata file exists
            if metadata.is_metadata_file_path_exists():
                metadata_info = metadata.read_metadata_info()
                #this means that we need to run it from the date it was last run
                from_date = metadata_info.to_date
            

            data_ingestion_config = DataIngestionConfig(
                from_date = from_date, 
                to_date = to_date, 
                data_ingestion_dir = data_ingestion_dir, 
                download_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_DOWNLOADED_DATA_DIR), 
                file_name = DATA_INGESTION_FILE_NAME, 
                feature_store_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_FEATURE_STORE_DIR), 
                failed_dir = os.path.join(data_ingestion_dir,DATA_INGESTION_FAILED_DIR), 
                metadata_file_path = metadata_file_path, 
                datasource_url = DATA_INGESTION_DATA_SOURCE_URL)


            logger.log(f"Data Ingestion config ,{data_ingestion_config}")    

            return data_ingestion_config
                
        except Exception as e:
            raise FinanceException(e, sys)    


