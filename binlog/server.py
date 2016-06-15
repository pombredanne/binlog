from io import BytesIO
import os
import asyncio
import signal

from .writer import TDSWriter


class Server:
    def __init__(self, base, uds_path):
        self.binlog = TDSWriter(base)
        self.uds_path = uds_path
        self._proto = None

    def get_protocol(self):
        if self._proto is None:
            class BinlogProtocol(asyncio.Protocol):
                def __init__(_self, *args, **kwargs):
                    _self._buf = None
                    super().__init__(*args, **kwargs)

                def connection_made(_self, transport):
                    _self._buf = BytesIO()

                def data_received(_self, data):
                    if _self._buf is not None:
                        _self._buf.write(data)
                    else:
                        raise RuntimeError(
                            "Data received after connection opening "
                            "(reused protocol)")

                def connection_lost(_self, exc):
                    if _self._buf.tell():
                        self.binlog.append(_self._buf.getvalue())
                    _self._buf = None

            self._proto = BinlogProtocol

        return self._proto

    def run(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        server = loop.run_until_complete(
            loop.create_unix_server(self.get_protocol(),
                                    self.uds_path))

        loop.add_signal_handler(signal.SIGINT, loop.stop)
        loop.add_signal_handler(signal.SIGTERM, loop.stop)

        try:
            loop.run_forever()
        finally:
            # Close the server
            try:
                server.close()
                
                loop.run_until_complete(server.wait_closed())
                loop.close()
            finally:
                try:
                    os.unlink(self.uds_path)
                except IOError:
                    pass
