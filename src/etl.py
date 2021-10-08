import csv
import glob
import os

import create_tables
from sql_queries import *


def create_event_data_csv(event_data_relative_path, event_data_csv_filename):
    file_path_list = get_events_file_paths(event_data_relative_path)

    # print(len(file_path_list))
    # for every filepath in the file path list
    create_even_csv_datafile(file_path_list, event_data_csv_filename)


def get_events_file_paths(relative_data_path):
    # checking your current working directory
    # print(os.getcwd())
    # Get your current folder and subfolder event data
    file_path_list = ""
    filepath = os.getcwd() + relative_data_path
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*'))
        # print(file_path_list)
    return file_path_list


def create_even_csv_datafile(file_path_list, file_name):
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csv_reader = csv.reader(csvfile)
            next(csv_reader)

            # extracting each data row one by one and append it
            for line in csv_reader:
                # print(line)
                full_data_rows_list.append(line)

            # uncomment the code below if you would like to get total number of rows
            # print(len(full_data_rows_list))
            # uncomment the code below if you would like to check to see what the list of event data rows will look like
            # print(full_data_rows_list)

            # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
            # Apache Cassandra tables
            csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

            with open(file_name, 'w', encoding='utf8', newline='') as file:
                writer = csv.writer(file, dialect='myDialect')
                writer.writerow(
                    ['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
                     'level', 'location', 'sessionId', 'song', 'userId'])
                for row in full_data_rows_list:
                    if row[0] == '':
                        continue
                    writer.writerow((
                        row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                        row[12], row[13], row[16]))


class ColumnNames:
    artist = "artist"
    first_name = "firstName"
    last_name = "lastName"
    gender = "gender"
    item_in_session = "itemInSession"
    length = "length"
    level = "level"
    location = "location"
    session_id = "sessionId"
    user_id = "userId"
    song = "song"


def insert_data(session, file):
    # "artist","firstName","gender",
    # "itemInSession",
    # "lastName","length","level",
    # "location","sessionId","song","userId"

    with open(file, encoding='utf8') as f:
        for line in csv.DictReader(f):
            values = (
                line[ColumnNames.artist],
                line[ColumnNames.song],
                float(line[ColumnNames.length]),
                int(line[ColumnNames.session_id]),
                int(line[ColumnNames.item_in_session])
            )
            print(values)
            session.execute(MUSIC_APP_HISTORY_INSERT_STATEMENT, values)
            values = (line[ColumnNames.artist],
                      line[ColumnNames.song],
                      float(line[ColumnNames.length]),
                      line[ColumnNames.first_name] + " " + line[ColumnNames.last_name],
                      int(line[ColumnNames.session_id]),
                      int(line[ColumnNames.item_in_session]),
                      int(line[ColumnNames.user_id]))
            #         print(values)
            session.execute(ARTIST_LISTENING_HISTORY_INSERT_STATEMENT, values)
            values = (line[ColumnNames.first_name] + " " + line[ColumnNames.last_name],
                      line[ColumnNames.song])
            #         print(values)
            session.execute(USER_LISTENING_HISTORY_INSERT_STATEMENT, values)


def select_statements(session):
    r = session.execute(SELECT_FROM_ARTIST_LISTENING_HISTORY_BY_USER_ID_AND_SESSION_ID,
                        (10, 182))
    for x in r:
        print(x)


def main():
    event_data_relative_path = '/event_data'
    event_data_csv_filename = 'event_datafile_new.csv'

    cluster, session = create_tables.create_keyspace()
    create_event_data_csv(event_data_relative_path, event_data_csv_filename)

    insert_data(session, event_data_csv_filename)
    select_statements(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
