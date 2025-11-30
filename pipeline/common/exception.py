
class FileNotFoundError(Exception):
        def __init__(self, value):
            message = f"File not found for {value}"
            super().__init__(message)