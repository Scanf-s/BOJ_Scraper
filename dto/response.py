class Response:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body

    def to_dict(self):
        return {"statusCode": self.status_code, "body": self.body}
