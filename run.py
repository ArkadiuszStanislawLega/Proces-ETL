from argparse import ArgumentParser
from sqlite3 import connect

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


def main():
    #parser = ArgumentParser(description='This is an example of sql API')
    #parser.add_argument('--path', dest='path', type=str, required=True)

    #args = parser.parse_args()

    with connect(DB_PATH) as db_connctor:
        db_connctor.execute(worker_tablse_stmt)

        db_cursor = db_connctor.cursor()
        db_cursor.executemany(insert_stmt, data_to_isert)

        for entry in db_cursor.execute('SELECT * FROM worker'):
            print(entry)


if __name__ == "__main__":
    main()
