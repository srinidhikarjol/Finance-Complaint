import os
import yaml
import shutil
from finance_complaint.exception import FinanceException


def write_yaml_file(file_path:str, data:dict):
    """
    Create and write data to yaml file
    """
    try:
        os.makedirs(name=file_path,exist_ok=True)
        with open(file_path,"w") as yaml_file:
            yaml.dump(data,yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)  


def read_yaml_file(file_path:str) -> dict:
    """
    Reads and returns data from a yaml file
    """
    try:
        with open(file_path,'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FinanceException(error_message = e, error_detail = sys)  