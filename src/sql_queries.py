class TableNames:
    music_app_history = "music_app_history"
    artist_listening_history = "artist_listening_history"
    user_listening_history = "user_listening_history"
    ALL_TABLES = [music_app_history, artist_listening_history, user_listening_history]


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
VALUES (?,?,?,?,?,?,?)
"""

USER_LISTENING_HISTORY_TABLE_CREATE_STATEMENT = f"""
CREATE TABLE {TableNames.user_listening_history}(
    username text,
    song text,
    PRIMARY KEY (song)
);
"""
USER_LISTENING_HISTORY_INSERT_STATEMENT = f"""
INSERT INTO {TableNames.user_listening_history}(username,song) VALUES (?,?)
"""

MUSIC_APP_HISTORY_TABLE_CREATE_STATEMENT = f"""
CREATE TABLE {TableNames.music_app_history}(
   artist text, 
   title text, 
   length float,
   session_id bigint,
   item_in_session int,
   PRIMARY KEY (session_id, item_in_session)
   )
"""
MUSIC_APP_HISTORY_INSERT_STATEMENT = f"""
INSERT INTO {TableNames.music_app_history}(artist, title, length, session_id, item_in_session) 
VALUES (?,?,?,?,?)
"""

# FIND
SELECT_FROM_USER_LISTENING_HISTORY_BY_SONG = f"""
SELECT username FROM {TableNames.user_listening_history}
WHERE song=?
"""
SELECT_FROM_MUSIC_APP_HISTORY_BY_SESSION_ID_AND_ITEM_IN_SESSION = f"""
SELECT artist, title, length FROM {TableNames.music_app_history}
 WHERE session_id = ? And item_in_session = ?
 """
SELECT_FROM_ARTIST_LISTENING_HISTORY_BY_USER_ID_AND_SESSION_ID = f"""
 SELECT artist, title, username  FROM {TableNames.artist_listening_history}
 WHERE user_id = ? and session_id = ?
"""

# QUERY LISTS
CREATE_TABLE_QUERIES = [MUSIC_APP_HISTORY_TABLE_CREATE_STATEMENT,
                        ARTIST_LISTENING_HISTORY_TABLE_CREATE_STATEMENT,
                        USER_LISTENING_HISTORY_TABLE_CREATE_STATEMENT]

DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {};"
DROP_TABLE_QUERIES = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.ALL_TABLES]
