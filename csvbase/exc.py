class CSVBaseException(Exception):
    """ABC for CSVBase exceptions to make it possible to catch them collectively"""

    pass


class TableDoesNotExistException(CSVBaseException):
    def __init__(self, username: str, table_name: str):
        self.table_name = table_name
        self.username = username
        super().__init__((username, table_name))


class UserDoesNotExistException(CSVBaseException):
    def __init__(self, username: str):
        self.username = username
        super().__init__(username)


class RowDoesNotExistException(CSVBaseException):
    def __init__(self, username: str, table_name: str, row_id: int):
        self.table_name = table_name
        self.username = username
        self.row_id = row_id
        super().__init__((username, table_name, row_id))


class NotAuthenticatedException(CSVBaseException):
    pass


class NotAllowedException(CSVBaseException):
    pass


class WrongAuthException(CSVBaseException):
    pass
