import os
import sys
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from mlopsdl.entity.config_entity import DataIngestionConfig
from mlopsdl.entity.artifact_entity import DataIngestionArtifact
from mlopsdl.exception import MLOpsException
from mlopsdl.logger import logging
from mlopsdl.data_access.mlopsdl_data import mlopsdlData

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig=DataIngestionConfig()):
        try:
            logging.info(f"{'>>'*20} Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
            self.mlopsdl_data = mlopsdlData()
        except Exception as e:
            raise MLOpsException(e, sys)
    
    def export_data_into_feature_store(self) -> DataFrame:
        try:
            logging.info(f"Exporting data from mongodb")
            mlopsdl_data = mlopsdlData()
            df = self.mlopsdl_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            logging.info(f"Exported collection data as dataframe with shape: {df.shape}")
            feature_store_file_path = self.data_ingestion_config.feature_store_filepath
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Saving dataframe to feature store file path: {feature_store_file_path}")
            df.to_csv(feature_store_file_path, index=False, header=True)
            return df
        
        except Exception as e:
            raise MLOpsException(e, sys)
        
    def split_data_as_train_test(self, df: DataFrame) -> None:
        logging.info("Entered the split_data_as_train_test method of DataIngestion class")

        try:
            train_set, test_set = train_test_split(df, test_size=self.data_ingestion_config.train_test_split_ratio)
            logging.info(f"Performed train test split with test size: {self.data_ingestion_config.train_test_split_ratio}")
            logging.info(f"Saving train and test data to file paths: {self.data_ingestion_config.training_file_path} and {self.data_ingestion_config.testing_file_path}")
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Exporting train and test data to file paths: {self.data_ingestion_config.training_file_path} and {self.data_ingestion_config.testing_file_path}")

            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)
            logging.info(f"Exported train and test data to file paths: {self.data_ingestion_config.training_file_path} and {self.data_ingestion_config.testing_file_path}")

        except Exception as e:
            raise MLOpsException(e, sys) from e
        
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            df = self.export_data_into_feature_store()
            self.split_data_as_train_test(df=df)
            data_ingestion_artifact = DataIngestionArtifact(training_file_path=self.data_ingestion_config.training_file_path, testing_file_path=self.data_ingestion_config.testing_file_path)
            logging.info(f"Data Ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise MLOpsException(e, sys) from e