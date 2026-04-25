import os
import sys

import numpy as np
import dill
import yaml
from pandas import DataFrame

from mlopsdl.exception import MLOpsException
from mlopsdl.logger import logging

def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its contents as a dictionary.
    
    :param file_path: The path to the YAML file.
    :return: A dictionary containing the contents of the YAML file.
    """
    try:
        with open(file_path, 'r') as yaml_file:
            content = yaml.safe_load(yaml_file)
            logging.info(f"Successfully read YAML file: {file_path}")
            return content
    except Exception as e:
        logging.error(f"Error reading YAML file: {file_path} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())
    
def write_yaml_file(file_path: str, content: dict) -> None:
    """
    Writes a dictionary to a YAML file.
    
    :param file_path: The path to the YAML file.
    :param content: The dictionary to write to the YAML file.
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok=True)
        with open(file_path, 'w') as yaml_file:
            yaml.safe_dump(content, yaml_file)
            logging.info(f"Successfully wrote to YAML file: {file_path}")
    except Exception as e:
        logging.error(f"Error writing to YAML file: {file_path} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())
    
def load_object(file_path: str):
    """
    Loads a Python object from a file using dill.
    
    :param file_path: The path to the file containing the serialized object.
    :return: The deserialized Python object.
    """
    logging.info("Entered the load_object method of utils")
    try:
        with open(file_path, 'rb') as file:
            obj = dill.load(file)
            logging.info(f"Successfully loaded object from file: {file_path}")
            return obj
    except Exception as e:
        logging.error(f"Error loading object from file: {file_path} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())
    
def save_numpy_array(file_path: str, array: np.ndarray) -> None:
    """
    Saves a NumPy array to a file.
    
    :param file_path: The path to the file where the array will be saved.
    :param array: The NumPy array to save.
    """
    logging.info("Entered the save_numpy_array method of utils")
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok=True)
        np.save(file_path, array)
        logging.info(f"Successfully saved NumPy array to file: {file_path}")
    except Exception as e:
        logging.error(f"Error saving NumPy array to file: {file_path} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())
    
def load_numpy_array(file_path: str) -> np.ndarray:
    """
    Loads a NumPy array from a file.
    
    :param file_path: The path to the file containing the saved NumPy array.
    :return: The loaded NumPy array.
    """
    logging.info("Entered the load_numpy_array method of utils")
    try:
        array = np.load(file_path)
        logging.info(f"Successfully loaded NumPy array from file: {file_path}")
        return array
    except Exception as e:
        logging.error(f"Error loading NumPy array from file: {file_path} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())
    
def save_object(file_path: str, obj) -> None:
    """
    Saves a Python object to a file using dill.
    
    :param file_path: The path to the file where the object will be saved.
    :param obj: The Python object to save.
    """
    logging.info("Entered the save_object method of utils")
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok=True)
        with open(file_path, 'wb') as file:
            dill.dump(obj, file)
            logging.info(f"Successfully saved object to file: {file_path}")
    except Exception as e:
        logging.error(f"Error saving object to file: {file_path} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())
    
def drop_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Drops specified columns from a DataFrame.
    
    :param df: The input DataFrame.
    :param columns: A list of column names to drop from the DataFrame.
    :return: A new DataFrame with the specified columns dropped.
    """
    try:
        print(f"Dropping columns: {columns} from DataFrame")
        df_dropped = df.drop(columns=columns)
        logging.info(f"Successfully dropped columns: {columns}")
        return df_dropped
    except Exception as e:
        logging.error(f"Error dropping columns: {columns} - {str(e)}")
        raise MLOpsException(str(e), sys.exc_info())