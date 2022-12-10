from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
import os,sys
from collections import namedtuple
from finance_complaint.utils import read_yaml_file,write_yaml_file
DataIngestionMetadataInfo = namedtuple("DataIngestionMetadataInfo", ["from_date", "to_date", "data_file_path"])

"""
This class is to read and write meta data to a yaml file.
This yaml file will have the from and to date to get the data from the api.
This file will be always up to date.
"""
class DataIngestionMetaData:

    def __init__(self, metadata_file_path:str):
        self.metadata_file_path = metadata_file_path
    
    def is_metadata_file_path_exists(self):
        return os.path.exists(self.metadata_file_path)

    def write_metadata_info(self,from_date:str,to_date:str,data_file_path:str):
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date = from_date,
                to_date = to_date,
                data_file_path = data_file_path
            )
            write_yaml_file(file_path=self.metadata_file_path, data=metadata_info)
       
        except Exception as e:
            raise FinanceException(e, sys)  

    def read_metadata_info(self) -> DataIngestionMetadataInfo:
        try:
            if not self.is_metadata_file_path_exists():
                raise Exception("No metadata info available")
            else:
                metadata = read_yaml_file(file_path=self.metadata_file_path)
                metadata_info = DataIngestionMetadataInfo(**(metadata)) 
                logger.info(metadata)
                return metadata   
        except Exception as e:
            raise FinanceException(e, sys)                      