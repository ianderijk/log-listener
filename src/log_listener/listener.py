import logging
import logging.handlers
import pickle
import select
import socketserver
import struct
from typing import Any, cast


class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    def __init__(self) -> None:
        self.logs = {}

    def cache_full(self) -> bool:
        return len(self.logs) >= 50

    def write_logs(self) -> None:
        if not self.cache_full():
            return

    def handle(self) -> None:
        while True:
            chunk = self.connection.recv(4)  # 4 is number of bytes
            if len(chunk) < 4:
                break

            # Unpack the bytes into an integer
            slen = struct.unpack(">L", chunk)[0]
            chunk = self.connection.recv(slen)

            # Iterate until all of the chunk has been received
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))

            # Load and handle the payload
            obj = self.unpickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handle_log_record(record)

    def unpickle(self, data):
        return pickle.loads(data)

    def handle_log_record(self, record):
        name = record.name

        logger = logging.getLogger(name)
        logger.handle(record)


class LogRecordSocketReceiver(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(
        self,
        host: str = "localhost",
        port: int = logging.handlers.DEFAULT_TCP_LOGGING_PORT,
        handler=LogRecordStreamHandler,
    ):
        socketserver.ThreadingTCPServer.__init__(self, (host, port), cast(Any, handler))
        self.abort = 0
        self.timeout = 1
        self.logname = None

    def serve_until_stopped(self):
        abort = 0
        while not abort:
            rd, _, _ = select.select([self.socket.fileno()], [], [], self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort


def main():
    logging.basicConfig(
        format="%(relativeCreated)5d %(name)-15s %(levelname)-8s %(message)s"
    )
    tcpserver = LogRecordSocketReceiver()
    print("Starting tcp server")
    tcpserver.serve_until_stopped()


if __name__ == "__main__":
    main()
