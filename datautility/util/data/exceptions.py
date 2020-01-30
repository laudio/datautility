''' All custom exceptions. '''


class ValidationError(RuntimeError):
    def __init__(self, message):
        super().__init__(message)
