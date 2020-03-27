class Track:
    def __init__(self, track_id, execution_id, performer, title):
        self.__track_id = track_id
        self.__exeution_id = execution_id
        self.__performer = performer
        self.__title = title

    def __str__(self):
        return f'Identyfikator utworu: {self.__track_id}, identyfikator wykonania: {self.__exeution_id}, autor: {self.__performer}, tytuł: {self.__title}'
