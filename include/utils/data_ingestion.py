import re
import shutil
import logging
from dotenv import load_dotenv
import os

load_dotenv()
ENV = os.getenv("ENV")
if ENV == "LOCAL":
    SOURCE_PATH = os.getenv("SOURCE_PATH")
    LANDING_PATH = os.getenv("LANDING_PATH")
    REJECTED_PATH = os.getenv("REJECTED_PATH")
    VALIDATED_PATH = os.getenv("VALIDATED_PATH")
elif ENV == "DOCKER":
    SOURCE_PATH = os.getenv("DOCKER_SOURCE_PATH")
    LANDING_PATH = os.getenv("DOCKER_LANDING_PATH")
    REJECTED_PATH = os.getenv("DOCKER_REJECTED_PATH")
    VALIDATED_PATH = os.getenv("DOCKER_VALIDATED_PATH")
    
FILE_NAME_PATTERN = r"^sales_[a-zA-Z]+_[a-zA-Z0-9_]+_\d{8}\.csv$"

def valid_file_extension(file_name:str)->bool:
    if file_name.endswith(".csv"):
        return True

    return False


def file_name_pattern_validation(file_name:str)->bool:
    if re.match(FILE_NAME_PATTERN, file_name):
        return True
    return False

def rejected_text_file_generation(source_file:str, message:str)->None:
    file_name = source_file.split(".")[0]
    with open(f"{REJECTED_PATH}/{file_name}.txt", "w") as f:
        f.write(f"{source_file} {message}")
    return None

def move_file_to_targeted_folder(source_path:str, destination_path:str, file_name:str)->None:
    complete_file_path = os.path.join(source_path, file_name)
    if os.path.isfile(complete_file_path):
        shutil.move(complete_file_path, destination_path)
    return None

def ingestion_process()->None:
    logging.info(f"SOURCE_PATH: {SOURCE_PATH}")
    logging.info(f"LANDING_PATH: {LANDING_PATH}")
    logging.info(f"REJECTED_PATH: {REJECTED_PATH}")
    source_files = os.listdir(SOURCE_PATH)
    logging.info(f"source_files: {source_files}")

    for source_file in source_files:
        logging.info(f"Processing file: {source_file}")
        is_valid_extension = valid_file_extension(source_file)
        logging.info(f"Extension valid: {is_valid_extension}")
        if not is_valid_extension:
            message = "is not a valid file extension"
            rejected_text_file_generation(source_file, message)
            move_file_to_targeted_folder(SOURCE_PATH, REJECTED_PATH, source_file)
            logging.info(f"Moved rejected file: {source_file}")
            continue
        is_valid_name_pattern = file_name_pattern_validation(source_file)
        logging.info(f"File name valid: {is_valid_name_pattern}")
        if not is_valid_name_pattern:
            message = "is not a valid file name"
            rejected_text_file_generation(source_file, message)
            move_file_to_targeted_folder(SOURCE_PATH, REJECTED_PATH, source_file)
            logging.info(f"Moved rejected file: {source_file}")
            continue
        move_file_to_targeted_folder(SOURCE_PATH, LANDING_PATH, source_file)
        logging.info(f"Moved valid file to landing: {source_file}")


if __name__ =="__main__":
    ingestion_process()
