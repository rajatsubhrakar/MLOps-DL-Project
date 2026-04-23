import sys

from mlopsdl.exception import MLOpsException
from mlopsdl.logger import logging
from mlopsdl.components.data_ingestion import DataIngestion
from mlopsdl.entity.config_entity import DataIngestionConfig
from mlopsdl.entity.artifact_entity import DataIngestionArtifact

class TrainingPipeline:
    def __init__(self):
        pass
    
    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion_config = DataIngestionConfig()
            logging.info(f"Data Ingestion config: {data_ingestion_config}")
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info(f"Data Ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise MLOpsException(e, sys) from e
        

    def run_pipeline(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise MLOpsException(e, sys) from e