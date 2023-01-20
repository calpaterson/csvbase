from typing import Optional

from .value_objs import ColumnType, KeySet


class CSVBaseException(Exception):
    """ABC for CSVBase exceptions to make it possible to catch them collectively"""


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


class PageDoesNotExistException(CSVBaseException):
    def __init__(self, username: str, table_name, keyset: KeySet):
        self.table_name = table_name
        self.username = username
        self.keyset = keyset
        super().__init__((username, table_name, keyset))


class NotAuthenticatedException(CSVBaseException):
    pass


class NotAllowedException(CSVBaseException):
    pass


class WrongAuthException(CSVBaseException):
    pass


class InvalidAPIKeyException(CSVBaseException):
    pass


class InvalidRequest(CSVBaseException):
    pass


class CantNegotiateContentType(CSVBaseException):
    def __init__(self, supported):
        super().__init__(supported)


class WrongContentType(CSVBaseException):
    def __init__(self, supported, recieved):
        super().__init__((supported, recieved))


class UsernameAlreadyExistsException(CSVBaseException):
    def __init__(self, username: str):
        self.username = username


class ProhibitedUsernameException(CSVBaseException):
    pass


class UnconvertableValueException(CSVBaseException):
    # FIXME: this also undoubtably needs row and column name
    def __init__(self, expected_type: ColumnType, string: str):
        self.expected_type = expected_type
        self.string = string


class CSVException(CSVBaseException):
    def __init__(self, message: str):
        self.message = message
