from argparse import ArgumentParser
from sqlite3 import connect
from Helpers.printer import SpecialPrinter
import datetime
import os

DB_PATH = "etl.db"
# Katalog z którego ma zostać pobrany plik do dodania
FILE_PATH = 'F:\\zadanie_python\\'

TRACK_FILE_NAME = 'unique_tracks.txt'
TRIPLETS_SAMPLE_FILE_NAME = 'triplets_sample_20p.txt'

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
            # db_cursor.executemany(insert_stmt, data_to_isert)

            data_list = []
            start_time = datetime.datetime.now()
            last_item = 0
            with open(FULL_FILE_TRIPLETS_SAMPLE_PATH, 'r', encoding='ANSI') as file:
                print(
                    "Próba otwarcia pliku przebiegła pomyślnie. Przetwarzam, proszę czekać ...")
                for i, line in enumerate(file):
                    try:
                        row = line.split("<SEP>")
                        data_list.append((row[0], row[1], row[2]))
                        if i % 1000 == 0:
                            db_cursor.executemany(
                                f'INSERT INTO {SAMPLES_TABLE}({USER_ID_COLUMN}, {TRACK_ID_COLUMN}, {LISTENING_DATE_COLUMN}) VALUES(?, ?, ?)', data_list)
                            data_list.clear()
                        last_item = i
                    except IndexError:
                        break
                print_surround(
                    f'Dadano {last_item} wierszy do bazy danych w czasie {datetime.datetime.now() - start_time}')
    except FileNotFoundError:
        print("Nie ma takiego pliku.")
    except UnicodeDecodeError:
        print("Błąd typu kodowania pliku.")


def read_track_file():
    print_surround("Dodawanie utworów")
    try:
        print(f'Dodaje utwory do bazy danych z pliku: {FULL_FILE_TRACK_PATH}')
        with connect(DB_PATH) as db_connctor:
            db_connctor.execute(table_track)

            db_cursor = db_connctor.cursor()
            # db_cursor.executemany(insert_stmt, data_to_isert)

            data_list = []
            start_time = datetime.datetime.now()
            last_item = 0
            with open(FULL_FILE_TRACK_PATH, 'r', encoding='ANSI') as file:
                print(
                    "Próba otwarcia pliku przebiegła pomyślnie. Przetwarzam, proszę czekać ...")
                for i, line in enumerate(file):
                    try:
                        row = line.split("<SEP>")
                        data_list.append((row[0], row[1], row[2], row[3]))
                        if i % 1000 == 0:
                            db_cursor.executemany(
                                f'INSERT INTO {TRACK_TABLE}({TRACK_ID_COLUMN}, {EXECUTION_ID_COLUMN}, {ARTIS_NAME_COLUMN}, {TRACK_TITLE_COLUMN}) VALUES(?, ?, ?, ?)', data_list)
                            data_list.clear()
                        last_item = i
                    except IndexError:
                        break
                print_surround(
                    f'Dadano {last_item} wierszy do bazy danych w czasie {datetime.datetime.now() - start_time}')
    except FileNotFoundError:
        print("Nie ma takiego pliku.")
    except UnicodeDecodeError:
        print("Błąd typu kodowania pliku.")


def print_surround(information: str):
    SpecialPrinter.surrounded_text(information, 80, " ", "-")


if __name__ == "__main__":
    main()
