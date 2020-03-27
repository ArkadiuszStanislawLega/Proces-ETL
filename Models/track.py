class Track:
    def __init__(self, track_id, execution_id, performer, title):
        self.__track_ID = track_id
        self.__exeution_ID = execution_id
        self.__performer = performer
        self.__title = title

    def __str__(self):
        return f'Identyfikator utworu: {self.__track_ID}, identyfikator wykonania: {self.__exeution_ID}, autor: {self.__performer}, tytuł: {self.__title}'
