from pilake import lake_utils

LISTENING_HOST = "192.168.0.29"


class LogWriter:
    def __init__(self, project: str, bucket: str = "logs") -> None:
        self.project = project
        self.bucket = bucket

    def _file_exists(self) -> bool:
        files = lake_utils.list_files(self.bucket)
        return any(f"{self.project}.log" in x for x in files)


# def write_log(log: str, bucket: str, project: str) -> None:
#     log_file =


print(lake_utils.list_files("rpi-logstreamer"))
