from datawarehouse.data_utils import get_conn_cursor,close_conn_cursor,create_schema,create_table,get_video_ids
from datawarehouse.data_load import data_load
from datawarehouse.data_trasnformation import parse_duration, transformation
from datawarehouse.modifications import insert_row,updated_rows,delete_row

import logging
from airflow.decorators import task 


logger = logging.getLogger(__name__)
table = "youtube_data" 

@task
def staging_table():
    
    schema = 'staging'
    conn,cursor = None, None


    try:
        conn,cursor = get_conn_cursor()

        youtube_data = data_load() 

        create_schema(schema)
        create_table(schema)

        table_id = get_video_ids(cursor,schema)


        for row in youtube_data:

            if len(table_id) != 0 and row['video_id'] in table_id:
                updated_rows(cursor, conn, schema, row)
            else:
                insert_row(schema, cursor,conn,row)


        ids_in_json = (row['video_id'] for row in youtube_data)

        if ids_to_delete := set(table_id) - ids_in_json:
            delete_row(cursor, conn, schema,ids_to_delete)

        logger.info(f"{schema} Table update completed ")


    except Exception as e:

        logger.error(f"error update in {schema} table : {e}")
        raise e 

    finally: 
        if conn and cursor:
            close_conn_cursor(conn, cursor)
        

        
@task

def core_table():
    
    schema = 'core'
    conn,cursor = None, None 
    
    
    try:
        
        conn,cursor = get_conn_cursor()
        
        create_schema()
        create_table()
        
        table_id = get_video_ids(cursor, schema) 
        
        current_video_ids = set()
        
        cursor.execute(f" SELECT * FROM staging .{schema};")
        rows = cursor.fetchall()
        
        
        for row in rows:
            current_video_ids.add(row["Video_ID"])
        
    
            if len(table_id) == 0:
                transformed_row = transformation(row)
                insert_row(schema,cursor,conn,row)
                
            else:
                transformed_row = transformation(row)
                
                
                if transformed_row['Video_ID'] in table_id:
                    updated_rows(cursor,conn,schema,row)
                    
                    
                else:
                    insert_row(schema,cursor,conn,row)
                    
            ids_to_delete = set(table_id) - current_video_ids
        
        if ids_to_delete:
            delete_row(cursor,conn,schema,ids_to_delete)
        logger.info(f"{schema} Table update completed ")

            
    except Exception as e:
        
        logger.error(f"error update in {schema} table : {e}")
        raise e 
    
    finally: 
        if conn and cursor:
            close_conn_cursor(conn, cursor)
        
        
        