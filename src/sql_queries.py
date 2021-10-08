class TableNames:
    TIME = "time"
    ARTISTS = "artists"
    SONGS = "songs"
    USERS = "users"
    SONGPLAYS = "songplays"
    ALL_TABLES = [TIME, ARTISTS, SONGPLAYS, SONGS, USERS]


DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {};"

# DROP TABLES

SONGPLAY_TABLE_DROP = DROP_TABLE_QUERY_TEMPLATE.format(TableNames.SONGPLAYS)

# CREATE TABLES

# alter table time owner to postgres;

SONGPLAY_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.SONGPLAYS}(
    id uuid not null constraint songplays_pk primary key,
    start_time timestamp not null constraint songplays__time_fk references time,
    user_id bigint constraint songplays__user_fk references users,
    level varchar,
    song_id varchar constraint songplays__songs_fk references songs,
    artist_id varchar constraint songplays__artist_fk references artists,
    session_id bigint,
    location varchar,
    user_agent varchar
    );
""")

# FIND SONGS
SONG_SELECT = (f"""
    SELECT s.song_id, s.artist_id,s.title,a.name,s.duration FROM {TableNames.SONGS} s
    JOIN {TableNames.ARTISTS} a ON s.artist_id=a.artist_id
    WHERE (s.title,a.name,s.duration) in ({{}}) 
    """)

# QUERY LISTS
CREATE_TABLE_QUERIES = []
DROP_TABLE_QUERIES = []
