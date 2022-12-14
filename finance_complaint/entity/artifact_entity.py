from dataclasses import dataclass
"""
artifact objects
"""
#Data Ingestion artifact
@dataclass
class DataIngestionArtifact:
    feature_store_file_path:str
    metadata_file_path:str
    download_dir:str
