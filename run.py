from argparse import ArgumentParser
from sqlite3 import connect
from Models.track import Track
from Models.sample import Sample

worker_tablse_stmt = """
    CREATE TABLE IF NOT EXISTS track(
        userId VARCHAR(30),
        trackId VARCHAR(20),
        listeningDate VARCHAR(20)
    )"""

insert_stmt = 'INSERT INTO worker(name, salary) VALUES(?,?)'
data_to_isert = [
    ('name1', 100),
    ('name2', 200),
    ('name3', 300),
    ('name4', 400),
    ('name5', 500),
    ('name6', 600)
]

DB_PATH = "etl.db"
FILE_PATH = 'F:\\zadanie_python\\'

TRACK_FILE_NAME = 'unique_tracks.txt'
TRIPLETS_SAMPLE_FILE_NAME = 'triplets_sample_20p.txt'

FULL_FILE_TRACK_PATH = f'{FILE_PATH}{TRACK_FILE_NAME}'
FULL_FILE_TRIPLETS_SAMPLE_PATH = f'{FILE_PATH}{TRIPLETS_SAMPLE_FILE_NAME}'


def tutoria_db():
    parser = ArgumentParser(description='This is an example of sql API')
    parser.add_argument('--path', dest='path', type=str, required=True)

    args = parser.parse_args()

    with connect(DB_PATH) as db_connctor:
        db_connctor.execute(worker_tablse_stmt)

        db_cursor = db_connctor.cursor()
        #db_cursor.executemany(insert_stmt, data_to_isert)

        data_list = []
        with open(FULL_FILE_TRIPLETS_SAMPLE_PATH, 'r', encoding='ANSI') as file:
            for i, line in enumerate(file):
                try:
                    row = line.split("<SEP>")
                    data_list.append((row[0], row[1], row[2]))
                    if i % 100 == 0:
                        db_cursor.executemany(
                            'INSERT INTO track(userId, trackId, listeningDate) VALUES(?, ?, ?)', data_list)
                        data_list.clear()
                        print(f'{i}. Czyszcze pamięć.')
                except IndexError:
                    break


def main():
    tutoria_db()


def read_triplets_sample():
    table = []
    try:
        with open(FULL_FILE_TRIPLETS_SAMPLE_PATH, 'r', encoding='ANSI') as file:
            counter = 0
            for couting in range(200):
                counter += 1
                for item in range(counter * 1000):
                    row = file.readline().split("<SEP>")
                    sample = Sample(row[0], row[1], row[2])
                    table.append(sample)
                print(f'{counter * 1000}')

    except FileNotFoundError:
        print("Nie ma takiego pliku.")
    except UnicodeDecodeError:
        print("Błąd typu kodowania pliku.")


def read_track_file():
    try:
        with open(FULL_FILE_TRACK_PATH, 'r', encoding='ANSI') as file:
            counter = 0
            for couting in range(200):
                counter += 1
                for item in range(counter * 1000):
                    try:
                        row = file.readline().split("<SEP>")
                        track = Track(row[0], row[1], row[2], row[3])
                    except IndexError:
                        print(len(file.readlines()))

                print(f'{counter * 1000}')

    except FileNotFoundError:
        print("Nie ma takiego pliku.")
    except UnicodeDecodeError:
        print("Błąd typu kodowania pliku.")


if __name__ == "__main__":
    main()
