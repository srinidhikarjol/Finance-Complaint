from collections import namedtuple
"""
configuration objects
"""
TrainingPipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

DataIngestionConfig = namedtuple(typename="DataIngestionConfig", field_names=[
    "from_date",
    "to_date",
    "data_ingestion_dir",
    "download_dir",
    "file_name",
    "feature_store_dir",
    "failed_dir",
    "metadata_file_path",
    "datasource_url"
])
