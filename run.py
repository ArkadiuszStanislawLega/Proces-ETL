from argparse import ArgumentParser
from sqlite3 import connect
from Models.track import Track

worker_tablse_stmt = """
    CREATE TABLE IF NOT EXISTS worker(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(20),
        salary INTEGER
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
TRACK_FILE_PATH = 'G:\\zadanie_python\\'
TRACK_FILE_NAME = 'unique_tracks.txt'
FULL_FILE_TRACK_PATH = f'{TRACK_FILE_PATH}{TRACK_FILE_NAME}'


def tutoria_db():
    #parser = ArgumentParser(description='This is an example of sql API')
    #parser.add_argument('--path', dest='path', type=str, required=True)

    #args = parser.parse_args()

    with connect(DB_PATH) as db_connctor:
        db_connctor.execute(worker_tablse_stmt)

        db_cursor = db_connctor.cursor()
        db_cursor.executemany(insert_stmt, data_to_isert)

        for entry in db_cursor.execute('SELECT * FROM worker'):
            print(entry)


def main():
    read_track_file()


def read_track_file():
    try:
        with open(FULL_FILE_TRACK_PATH, 'r', encoding='ANSI') as file:
            for item in range(10):
                row = file.readline().split("<SEP>")
                track = Track(row[0], row[1], row[2], row[3])
                print(track)

    except FileNotFoundError:
        print("Nie ma takiego pliku.")
    except UnicodeDecodeError:
        print("Błąd typu kodowania pliku.")


if __name__ == "__main__":
    main()
