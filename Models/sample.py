import datetime


class Sample:
    def __init__(self, userId, trackId, listeningDate):
        self.__user_id = userId
        self.__track_id = trackId
        self.__listening_date = listeningDate

    @property
    def userId(self):
        return self.__user_id

    @property
    def trackId(self):
        return self.__track_id

    @property
    def listeningDate(self):
        return self.__listening_date

    def __str__(self):
        return f'ID użytkownika: {self.__user_id}, ID utworu: {self.__track_id}, data: {self.__listening_date}'
