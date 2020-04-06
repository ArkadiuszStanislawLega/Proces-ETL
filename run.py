from argparse import ArgumentParser
from sqlite3 import connect
from Helpers.printer import SpecialPrinter
import datetime

parser = ArgumentParser(
    description='W aplikacji nazwy plików tekstowych z których mają być pobrane dane do bazy danych są już ustawione, należy tylko podać miejsce w którym się znajdują oraz nazwę bazy danych.')
parser.add_argument('--path',
                    dest='path',
                    type=str,
                    required=True,
                    help='Wyznacza miejsce z którego mają być pobrane pliki. Musi zostać zakończone podwójnym znakiem\'\\\' np. --path=\"C:\\Pliki do pobrania\\\\\"')

parser.add_argument('--db',
                    dest='db',
                    type=str,
                    required=True,
                    help='Nazwa pliku z bazą danych jaka ma zostać utworzona np. --db=\"baza.db\"')

args = parser.parse_args()

# Baza danych do której mają zotać zapisane dane
db_path = args.db
# Katalog z którego ma zostać pobrany plik do dodania
file_path = args.path

# Nazwy plików  z których są pobierane dane
TRACK_FILE_NAME = 'unique_tracks.txt'
TRIPLETS_SAMPLE_FILE_NAME = 'triplets_sample_20p.txt'

# Pełne ścieżki plików do dodania
FULL_FILE_TRACK_PATH = f'{file_path}{TRACK_FILE_NAME}'
FULL_FILE_TRIPLETS_SAMPLE_PATH = f'{file_path}{TRIPLETS_SAMPLE_FILE_NAME}'

# Liczba wierszy po dodaniu której zwalniam pamięć
NUMBER_OF_LINES_AFTER_MEMORY_FREED = 1000
# Separator danych który oddziela dane w wierszu pliku
SEPARATOR_CHARACTER = "<SEP>"

# Nazwy tabel
TRACK_TABLE = "track"
SAMPLES_TABLE = "sample"

# Nazwy kolumn w tabelach
ARTIS_NAME_COLUMN = "artistName"
EXECUTION_ID_COLUMN = "executionId"
LISTENING_DATE_COLUMN = "listeningDate"
TRACK_ID_COLUMN = "trackId"
TRACK_TITLE_COLUMN = "title"
USER_ID_COLUMN = "userId"

# Polecenia tworzenia tabel
table_track = f"""
    CREATE TABLE IF NOT EXISTS {TRACK_TABLE}(
        {TRACK_ID_COLUMN} VARCHAR(20),
        {EXECUTION_ID_COLUMN} VARCHAR(20),
        {ARTIS_NAME_COLUMN} VARCHAR(20),
        {TRACK_TITLE_COLUMN} VARCHAR(20)
    )"""

table_sample = f"""
    CREATE TABLE IF NOT EXISTS {SAMPLES_TABLE}(
        {USER_ID_COLUMN} VARCHAR(20),
        {TRACK_ID_COLUMN} VARCHAR(20),
        {LISTENING_DATE_COLUMN} VARCHAR(20)
    )"""

# Wiadomości do użytkownika
FILE_NOT_FOUND_ERROR_MESSAGE = "Nie ma takiego pliku."
DECODE_ERROR_MESSAGE = "Błąd typu kodowania pliku."
OPENING_FILE_WAS_SUCCESSFUL_MESSAGE = "Próba otwarcia pliku przebiegła pomyślnie. Przetwarzam, proszę czekać ..."

# Polecenia bazy danych
TRACK_INSERT_SQL_COMMAND = f'INSERT INTO {TRACK_TABLE}({EXECUTION_ID_COLUMN}, {TRACK_ID_COLUMN}, {ARTIS_NAME_COLUMN}, {TRACK_TITLE_COLUMN}) VALUES(?, ?, ?, ?)'
SAMPLES_INSERT_SQL_COMMAND = f'INSERT INTO {SAMPLES_TABLE}({USER_ID_COLUMN}, {TRACK_ID_COLUMN}, {LISTENING_DATE_COLUMN}) VALUES(?, ?, ?)'
SELECT_ARTIST_MOST_OFTEN_LISTENED = f"SELECT {TRACK_TABLE}.{ARTIS_NAME_COLUMN}, COUNT({SAMPLES_TABLE}.{TRACK_ID_COLUMN}) AS `num` FROM {SAMPLES_TABLE} INNER JOIN {TRACK_TABLE} ON {SAMPLES_TABLE}.{TRACK_ID_COLUMN}={TRACK_TABLE}.{TRACK_ID_COLUMN}  GROUP BY {SAMPLES_TABLE}.{TRACK_ID_COLUMN} HAVING `num` > 1 ORDER BY `num` DESC LIMIT 1 "
SELECT_FIVE_MOST_LISTENED_SONGS = f"SELECT {TRACK_ID_COLUMN}, COUNT({TRACK_ID_COLUMN}) AS `num` FROM {SAMPLES_TABLE} GROUP BY {TRACK_ID_COLUMN} HAVING `num` > 1 ORDER BY `num` DESC LIMIT 5"
# Ustawienia tabel
CHARACTER_THAT_BUILD_TABLES = "-"

# Jeśli zostanie zmieniona długość ogólna tabel,
# to należy też zmienić proporcjonalnie:
# - ARTIST_AND_TITLE_COLUMN_WIDTH
# - TIME_COUNTER_COLUMN_WIDTH
# Należy pamiętać że w szerokość kolumny wchodzą znaki pionowe "|"
# pomiędzy kolumnami, oraz spacja na początku kolumny jak i na końcu.
MAX_TABLE_WIDTH = 80

# Szerokość kolumny nazwy artysty i tytułu utworu, ma wpływ
# na długość wyświetlanej nazwy i tytyułu w tabeli.
ARTIST_AND_TITLE_COLUMN_WIDTH = 25
TIME_COUNTER_COLUMN_WIDTH = 15


def main():
    read_track_file()
    read_triplets_sample()
    find_best_artist()
    find_five_tracks()


def execute_select(select_command: str):
    """
    Wykonuje zapytanie do bazy podane w argumencie.
    Arguments:
        select_command {str} -- zapytanie do bazy danych 
    """
    start = datetime.datetime.now()
    with connect(db_path) as db_connector:
        db_cursor = db_connector.cursor()
        exucuted_select = db_cursor.execute(select_command)
        print_surround(f'Czas przeszukiwania bazy danych: {datetime.datetime.now()-start}')

        return exucuted_select.fetchall()


def find_best_artist():
    """
    Przeszukuje bazę danych w celu znalezienia najczęściej odsłuchiwanego autora.
    Używa polecenia SQL w którym jest zawarty INNER JOIN - przez co wolniej pracuje.
    """

    print("Przeszykuję bazę danych w celu odnalezienia autora z największą liczbą odsłuchań.\nUżyłem do tego polecenia \"INNER JOIN\" do następnych już nie, żeby pokazać różnicę\nw szybkości wyszukiwania. Czas wyszukiwania może zająć około 6min.\nProszę czekać...")
    executed = execute_select(SELECT_ARTIST_MOST_OFTEN_LISTENED)
    print(f'Autor z największą liczbą odsłuchań: {executed[0][0]}, ilość dosłuchań: {executed[0][1]}')


def find_five_tracks():
    """
    Przeszukuje baze danych w celu znalezienia 5 najczęściej odtwarzanych utworów.
    """
    print("Przeszukuję bazę w celu znalezienia 5 najczęściej odsłuchiwanych utworów.\nCzas wyszukiwania może zająć od ok. 30s nawet do 2min w zależności od sprzętu.\nProszę czekać...")
    fill_data_and_print(execute_select(SELECT_FIVE_MOST_LISTENED_SONGS))


def fill_data_and_print(list):
    """
    Uzupełnia dane podane wliście o dane z drugiej tabeli która zawiera nazwy zespołów i tytuły.
    (To rozwiązanie jest szybsze niż użycie inner join)
    Następnie drukuje tabele z wynikami i czasami.

    Arguments:
        list {list} -- lista z kluczami artystów
    """
    print("Uzupełniam dane o właściwe nazwy. Proszę czekać...")

    with connect(db_path) as db_connector:
        db_cursor = db_connector.cursor()

        # Drukowanie nagłówka tabeli
        print(MAX_TABLE_WIDTH*"-")
        print(f"|%1s| %{ARTIST_AND_TITLE_COLUMN_WIDTH}s | %{ARTIST_AND_TITLE_COLUMN_WIDTH}s |  %{TIME_COUNTER_COLUMN_WIDTH}s |" % ("Lp.", "Nazwa Artysty", "Tytuł", "Czas pobrań"))
        print(MAX_TABLE_WIDTH*CHARACTER_THAT_BUILD_TABLES)

        # Pobieranie z bazy danych kolejnych elemntów z listy i drukowanie ich do konsoli
        for i, row in enumerate(list):
            start = datetime.datetime.now()
            full_fill_row = db_cursor.execute(f"SELECT {ARTIS_NAME_COLUMN},{TRACK_TITLE_COLUMN} FROM {TRACK_TABLE} WHERE {TRACK_ID_COLUMN}=\'{row[0]}\'")
            end = datetime.datetime.now()

            values_from_row = full_fill_row.fetchone()

            title = short_string(values_from_row[1][:-1])
            author = short_string(values_from_row[0])

            elapsed_time = end-start

            print(f"| %1s | %{ARTIST_AND_TITLE_COLUMN_WIDTH}s | %{ARTIST_AND_TITLE_COLUMN_WIDTH}s |  %{TIME_COUNTER_COLUMN_WIDTH}s |" % (i+1, author, title, elapsed_time))

        # Drukowanie końca tabeli
        print(MAX_TABLE_WIDTH*CHARACTER_THAT_BUILD_TABLES)


def short_string(value: str):
    """
    Skraca podany w argumencie wyraz do wymiaru podanego w argumencie i dodaje trzy kropki.    
    Arguments:
        value {str} -- wyraz do skrócenia
    Returns:
        [str] -- podany wyraz w argumencie skrócony do rozmiaru podanego w argumencie
    """
    if len(value) > ARTIST_AND_TITLE_COLUMN_WIDTH:
        value = value[0:ARTIST_AND_TITLE_COLUMN_WIDTH-3:]
        value += "..."
    return value


def read_triplets_sample():
    """
    Pobiera dane z pliku tekstowego triplets_sample_20p.txt do bazy danych.
    """
    print("Dodawanie próbek")
    try:
        print(
            f'Dodaje próbki do bazy danych z pliku: {FULL_FILE_TRIPLETS_SAMPLE_PATH}')
        with connect(db_path) as db_connctor:
            db_connctor.execute(table_sample)
            db_cursor = db_connctor.cursor()
            data_list = []
            last_item = 0
            start_time = datetime.datetime.now()
            with open(FULL_FILE_TRIPLETS_SAMPLE_PATH, 'r', encoding='ANSI') as file:
                print(OPENING_FILE_WAS_SUCCESSFUL_MESSAGE)

                for i, line in enumerate(file):
                    row = line.split(SEPARATOR_CHARACTER)
                    data_list.append((row[0], row[1], row[2]))

                    if i % NUMBER_OF_LINES_AFTER_MEMORY_FREED == 0:
                        db_cursor.executemany(SAMPLES_INSERT_SQL_COMMAND, data_list)
                        data_list.clear()

                    last_item += 1

                print_end_adding(last_item, datetime.datetime.now()-start_time)

    except FileNotFoundError:
        print(FILE_NOT_FOUND_ERROR_MESSAGE)
    except UnicodeDecodeError:
        print(DECODE_ERROR_MESSAGE)


def read_track_file():
    """
    Pobiera dane z pliku tekstowego unique_tracks.txt do bazy danych.
    """
    print("Dodawanie utworów")
    try:
        print(f'Dodaje utwory do bazy danych z pliku: {FULL_FILE_TRACK_PATH}')
        with connect(db_path) as db_connctor:
            db_connctor.execute(table_track)
            db_cursor = db_connctor.cursor()
            data_list = []
            start_time = datetime.datetime.now()
            last_item = 0

            with open(FULL_FILE_TRACK_PATH, 'r', encoding='ANSI') as file:
                print(OPENING_FILE_WAS_SUCCESSFUL_MESSAGE)

                for i, line in enumerate(file):
                    row = line.split(SEPARATOR_CHARACTER)
                    data_list.append((row[0], row[1], row[2], row[3]))

                    if i % NUMBER_OF_LINES_AFTER_MEMORY_FREED == 0:
                        db_cursor.executemany(TRACK_INSERT_SQL_COMMAND, data_list)
                        data_list.clear()

                    last_item += 1

                print_end_adding(last_item, datetime.datetime.now()-start_time)

    except FileNotFoundError:
        print(FILE_NOT_FOUND_ERROR_MESSAGE)
    except UnicodeDecodeError:
        print(DECODE_ERROR_MESSAGE)


def print_surround(information: str):
    """
    Drukuje w konsoli podany w argumencie napis - wyśrodkowany otoczony myślnikami.
    Arguments:
        information {str} -- Napis który ma zostać wypisany w konsoli.
    """
    SpecialPrinter.surrounded_text(
        information, MAX_TABLE_WIDTH, " ", CHARACTER_THAT_BUILD_TABLES)


def print_end_adding(number_of_items: int, finish_time: datetime.datetime):
    """
    Drukuje w konsoli efekt dodawania wszystkich wierszy z pliku tekstowego do bazy danych.
    Podaje ilość wierszy oraz czas w któym zostało wykonane zadanie.
    Arguments:
        number_of_items {int} -- ilość wierszy dodana do bazy danych
        finish_time {datetime.datetime} -- czas w jakim zostały wykonane operacje.
    """
    print_surround(
        f'Dadano {number_of_items} wierszy do bazy danych w czasie {finish_time}')


if __name__ == "__main__":
    main()
