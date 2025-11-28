import json
import logging
from datetime import date 

logger = logging.getLogger(__name__) 

def data_load(): 
    
    file_path = f"./data/video_data_{date.today()}.json" 
    
    
    try: 
        
        logging.info(f"processing file at path: video_data_{(date.today())}") 
        
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
        
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {file_path}")
        raise
    
        