from argparse import ArgumentParser
from sqlite3 import connect
from Helpers.printer import SpecialPrinter
import datetime

DB_PATH = "etl.db"
# Katalog z którego ma zostać pobrany plik do dodania
FILE_PATH = 'F:\\zadanie_python\\'

TRACK_FILE_NAME = 'unique_tracks.txt'
TRIPLETS_SAMPLE_FILE_NAME = 'triplets_sample_20p.txt'

# Liczba wierszy po dodaniu której zwalniam pamięć
NUMBER_OF_LINES_AFTER_MEMORY_FREED = 1000

# region Pełne ścieżki plików do dodania
FULL_FILE_TRACK_PATH = f'{FILE_PATH}{TRACK_FILE_NAME}'
FULL_FILE_TRIPLETS_SAMPLE_PATH = f'{FILE_PATH}{TRIPLETS_SAMPLE_FILE_NAME}'
# endregion
# region Nazwy tabel
TRACK_TABLE = "track"
SAMPLES_TABLE = "sample"
# endregion
# region Nazwy kolumn w tabelach
ARTIS_NAME_COLUMN = "artistName"
EXECUTION_ID_COLUMN = "executionId"
LISTENING_DATE_COLUMN = "listeningDate"
TRACK_ID_COLUMN = "trackId"
TRACK_TITLE_COLUMN = "title"
USER_ID_COLUMN = "userId"
# endregion
# region Polecenia tworzenia tabel
table_track = f"""
    CREATE TABLE IF NOT EXISTS {TRACK_TABLE}(
        {TRACK_ID_COLUMN} VARCHAR(20),
        {EXECUTION_ID_COLUMN} VARCHAR(20),
        {ARTIS_NAME_COLUMN} VARCHAR(20),
        {TRACK_TITLE_COLUMN} VARCHAR(20)
    )"""

table_sample = f"""
    CREATE TABLE IF NOT EXISTS {SAMPLES_TABLE}(
        {USER_ID_COLUMN} VARCHAR(30),
        {TRACK_ID_COLUMN} VARCHAR(20),
        {LISTENING_DATE_COLUMN} VARCHAR(20)
    )"""
# endregion
# region Wiadomości do użytkownika
FILE_NOT_FOUND_ERROR_MESSAGE = "Nie ma takiego pliku."
DECODE_ERROR_MESSAGE = "Błąd typu kodowania pliku."
OPENING_FILE_WAS_SUCCESSFUL_MESSAGE = "Próba otwarcia pliku przebiegła pomyślnie. Przetwarzam, proszę czekać ..."
# endregion
# region Polecenia dodawania do bazy danych
TRACK_INSERT_SQL_COMMAND = f'INSERT INTO {TRACK_TABLE}({TRACK_ID_COLUMN}, {EXECUTION_ID_COLUMN}, {ARTIS_NAME_COLUMN}, {TRACK_TITLE_COLUMN}) VALUES(?, ?, ?, ?)'
SAMPLES_INSERT_SQL_COMMAND = f'INSERT INTO {SAMPLES_TABLE}({USER_ID_COLUMN}, {TRACK_ID_COLUMN}, {LISTENING_DATE_COLUMN}) VALUES(?, ?, ?)'
# endregion


def tutoria_db():
    parser = ArgumentParser(description='This is an example of sql API')
    parser.add_argument('--path', dest='path', type=str, required=True)

    args = parser.parse_args()

    with connect(DB_PATH) as db_connctor:
        # db_connctor.execute(worker_tablse_stmt)

        db_cursor = db_connctor.cursor()
        # db_cursor.executemany(insert_stmt, data_to_isert)


def main():
    read_track_file()
    print('\n\n\n')
    read_triplets_sample()


def read_triplets_sample():
    print_surround("Dodawanie próbek")
    try:
        print(
            f'Dodaje próbki do bazy danych z pliku: {FULL_FILE_TRIPLETS_SAMPLE_PATH}')
        with connect(DB_PATH) as db_connctor:
            db_connctor.execute(table_sample)
            db_cursor = db_connctor.cursor()
            data_list = []
            last_item = 0
            start_time = datetime.datetime.now()
            with open(FULL_FILE_TRIPLETS_SAMPLE_PATH, 'r', encoding='ANSI') as file:
                print(OPENING_FILE_WAS_SUCCESSFUL_MESSAGE)
                for i, line in enumerate(file):
                    row = line.split("<SEP>")
                    data_list.append((row[0], row[1], row[2]))
                    if i % NUMBER_OF_LINES_AFTER_MEMORY_FREED == 0:
                        db_cursor.executemany(
                            SAMPLES_INSERT_SQL_COMMAND, data_list)
                        data_list.clear()
                    last_item = i
                print_end_adding(last_item, datetime.datetime.now()-start_time)
    except FileNotFoundError:
        print(FILE_NOT_FOUND_ERROR_MESSAGE)
    except UnicodeDecodeError:
        print(DECODE_ERROR_MESSAGE)


def read_track_file():
    print_surround("Dodawanie utworów")
    try:
        print(f'Dodaje utwory do bazy danych z pliku: {FULL_FILE_TRACK_PATH}')
        with connect(DB_PATH) as db_connctor:
            db_connctor.execute(table_track)
            db_cursor = db_connctor.cursor()
            data_list = []
            start_time = datetime.datetime.now()
            last_item = 0
            with open(FULL_FILE_TRACK_PATH, 'r', encoding='ANSI') as file:
                print(OPENING_FILE_WAS_SUCCESSFUL_MESSAGE)
                for i, line in enumerate(file):
                    row = line.split("<SEP>")
                    data_list.append((row[0], row[1], row[2], row[3]))
                    if i % NUMBER_OF_LINES_AFTER_MEMORY_FREED == 0:
                        db_cursor.executemany(
                            TRACK_INSERT_SQL_COMMAND, data_list)
                        data_list.clear()
                    last_item = i
                print_end_adding(last_item, datetime.datetime.now()-start_time)
    except FileNotFoundError:
        print(FILE_NOT_FOUND_ERROR_MESSAGE)
    except UnicodeDecodeError:
        print(DECODE_ERROR_MESSAGE)


def print_surround(information: str):
    SpecialPrinter.surrounded_text(information, 80, " ", "-")


def print_end_adding(number_of_items: int, finish_time: datetime.datetime):
    print_surround(
        f'Dadano {number_of_items} wierszy do bazy danych w czasie {finish_time}')


if __name__ == "__main__":
    main()
