class TableNames:
    music_app_history = "music_app_history"
    artist_listening_history = "artist_listening_history"
    user_listening_history = "user_listening_history"
    all_tables = [music_app_history, artist_listening_history, user_listening_history]


# CREATE TABLES

ARTIST_LISTENING_HISTORY_TABLE_CREATE_STATEMENT = f"""
CREATE TABLE {TableNames.artist_listening_history}(
   artist text, 
   title text, 
   length float,
   username text,
   session_id int,
   item_in_session int,
   user_id int,
   PRIMARY KEY ((session_id,user_id),item_in_session)
   );
"""
ARTIST_LISTENING_HISTORY_INSERT_STATEMENT = f"""
INSERT INTO {TableNames.artist_listening_history}(artist, title, length, username,session_id, 
                                      item_in_session, user_id)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""

USER_LISTENING_HISTORY_TABLE_CREATE_STATEMENT = f"""
CREATE TABLE {TableNames.user_listening_history}(
    username text,
    song text PRIMARY KEY
);
"""
USER_LISTENING_HISTORY_INSERT_STATEMENT = f"""
INSERT INTO {TableNames.user_listening_history}(username,song) VALUES (%s,%s)
"""

MUSIC_APP_HISTORY_TABLE_CREATE_STATEMENT = f"""
CREATE TABLE {TableNames.music_app_history}(
   artist text, 
   title text, 
   length float,
   session_id bigint,
   item_in_session int,
   PRIMARY KEY (session_id,item_in_session)
   )
"""
MUSIC_APP_HISTORY_INSERT_STATEMENT = f"""
INSERT INTO {TableNames.music_app_history}(artist, title, length, session_id,item_in_session) 
VALUES (%s,%s,%s,%s,%s)
"""

# SELECTS
SELECT_FROM_USER_LISTENING_HISTORY_BY_SONG = f"""
SELECT username FROM {TableNames.user_listening_history}
WHERE song=%s
"""
SELECT_FROM_MUSIC_APP_HISTORY_BY_SESSION_ID_AND_ITEM_IN_SESSION = f"""
SELECT artist, title, length FROM {TableNames.music_app_history}
 WHERE session_id = %s AND item_in_session =%s
 """
SELECT_FROM_ARTIST_LISTENING_HISTORY_BY_USER_ID_AND_SESSION_ID = f"""
 SELECT artist, title, username  FROM {TableNames.artist_listening_history}
 WHERE user_id = %s and session_id = %s
"""

# QUERY LISTS
CREATE_TABLE_QUERIES = [MUSIC_APP_HISTORY_TABLE_CREATE_STATEMENT,
                        ARTIST_LISTENING_HISTORY_TABLE_CREATE_STATEMENT,
                        USER_LISTENING_HISTORY_TABLE_CREATE_STATEMENT]

DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {};"
DROP_TABLE_QUERIES = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.all_tables]
