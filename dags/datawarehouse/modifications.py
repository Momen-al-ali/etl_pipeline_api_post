import json
import logging
from datetime import date 



table = "youtube_data" 
logger = logging.getLogger(__name__) 


def insert_row(schema, cursor,conn,row):
    
    
    try:
        
        if schema == 'staging':
            cursor.execute(f"""INSERT INT {schema}.{table}(video_id, Video_title, Uploaded_date, Duration, View_count, Like_count, Comment_count);
                        VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)"""
                        ),row
        
        else:
            video_id = "Video_ID"
                        
            cursor.execute(f"""INSERT INT {schema}.{table}(video_id, Video_title, Uploaded_date, Duration, View_count, Like_count, Comment_count);
                        VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)"""
                        ),row
            
            
        conn.commit()
        logger.info(f"Insertes row with Video_ID : {row[video_id]}")
    
    except Exception as e:
        logger.error(f"Error inserting row with Video_ID : {row[video_id]} - {e}")
        raise e
    
        
        
def updated_rows(cursor, conn, schema, row):
    
    try:
        
    
        if schema == 'staging':
            video_id = 'Video_ID',
            Uploaded_date = 'publishedAt',
            Video_title = 'title',
            View_count = 'viewCount',
            Like_count = 'likeCount',
            Comment_count = 'commentCount'
            
            
        else: 
            
            video_id = 'Video_ID',
            upload_date = 'Upload_Date',
            video_title = 'Video_Title',
            video_views = 'Video_Views',
            likes_count = 'Likes_Count',
            comments_count = 'Comments_Count'
            
            
            cursor.execute(f"""
                        UPDATE {schema}.{table}
                        SET "Video_Title" = %({video_title})s,
                            "Video_Views" = %({video_views})s,
                            "Likes_Count" = %({likes_count})s,
                            "Comments_Count" = %({comments_count})s
                        WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;""", row
                        ),
            
        conn.commit
        logging.info(f"updated row with Video_ID : {row['video_id']}")

    except Exception as e:
        logging.error(f"error updating row with Video_ID : {row['video_id']}" - {e})
        raise e
    

def delete_row(cursor, conn, schema ,ids_to_delete): 
    
    try: 
        
        ids_to_delete= f"""({', '.join(f" '{id}' " for id in ids_to_delete)})"""
        
        cursor.execute(
        f"""
        DELETE FROM {schema}.{table} WHERE "Video_ID" IN {ids_to_delete};
        
        """
        )
    
        conn.commit()
        logging .info(f"Deleted row with Video_ID : {ids_to_delete}")
        
    
    except Exception as e:
        logging.error(f"error deleted row with Video_ID: {ids_to_delete}")
        raise e
    



    
    
    
    

        
        
        