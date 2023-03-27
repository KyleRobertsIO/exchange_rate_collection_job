class DatabaseQueryError(Exception):
    def __init__(self, message: str):
        super().__init__(
            "Query to database has failed. {0}".format(message)
        )