from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor  

table = "youtube_data" 





def get_conn_cursor(): 
    
    hooks = PostgresHook(postgres_conn_id= "postgres_db_yt_elt", database= "elt_db")
    conn = hooks.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor


def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close() 
    
    
    
def create_schema(schema):
    
    conn, cursor = get_conn_cursor()
    
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};" 
    
    cursor.execute(schema_sql)
    
    conn.commit()
    close_conn_cursor(conn, cursor)




def create_table(schema, table):
    
    conn, cursor = get_conn_cursor()
    
    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} 
        video_id VARCHAR(11) PRIMARY KEY,
        Video_title TEXT NOT NULL,
        Uploaded_date TIMESTAMP NOT NULL,
        Duration VARCHAR(20) NOT NULL,
        View_count INT,
        Like_count INT,
        Comment_count INT
        );  
        """
    
    else: 
        
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} 
        video_id VARCHAR(11) PRIMARY KEY,
        Video_title TEXT NOT NULL,
        Uploaded_date TIMESTAMP NOT NULL,
        video_type VARCHAR(50) NOT NULL,
        Duration VARCHAR(20) NOT NULL,
        View_count INT,
        Like_count INT,
        Comment_count INT
        );  
        """
        
    
    cursor.execute(table_sql)
    
    conn.commit()
    close_conn_cursor(conn, cursor)



def get_video_ids(cursor, schema):
    
    cursor.execute(f"SELECT video_id FROM staging.{schema}.{table};")
    ids = cursor.fetchall()
    
    video_ids = [row ['video_id'] for row in ids]
    
    return video_ids



