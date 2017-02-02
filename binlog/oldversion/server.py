from io import BytesIO
import asyncio
import logging
import os
import signal
import sys

from binlog.oldversion.writer import TDSWriter


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATS_INTERVAL = 1
GET_TIMEOUT = 0.5


class Server:
    def __init__(self, base, uds_path):
        self.binlog = TDSWriter(base)
        self.input_queue = asyncio.Queue()
        self.uds_path = uds_path
        self.writecount = 0
        self.closing = False
        self.loop = None
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
                        self.loop.call_soon_threadsafe(
                            self.input_queue.put_nowait,
                            _self._buf.getvalue())
                    _self._buf = None

            self._proto = BinlogProtocol

        return self._proto

    @asyncio.coroutine
    def log_stats(self):
        while not self.closing:
            yield from asyncio.sleep(STATS_INTERVAL)
            logger.info("Rows saved %d", self.writecount)
            self.writecount = 0

    @asyncio.coroutine
    def put_in_binlog(self):
        while not self.closing:
            try:
                data = yield from asyncio.wait_for(self.input_queue.get(),
                                                   GET_TIMEOUT)
            except asyncio.TimeoutError:
                pass
            else:
                self.binlog.append(data)
                self.writecount += 1

        if not self.input_queue.empty():
            logger.info("Flushing all pending elements in queue %d",
                        self.input_queue.qsize())
            try:
                while True:
                    data = self.input_queue.get_nowait()
                    self.binlog.append(data)
                    self.writecount += 1
            except asyncio.QueueEmpty:
                logger.info("Done flushing elements")


    def run(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        server = loop.run_until_complete(
            loop.create_unix_server(self.get_protocol(),
                                    self.uds_path,
                                    backlog=0))
        def stop_all(*_):
            self.closing = True
            server.close()

        loop.add_signal_handler(signal.SIGINT, stop_all)
        loop.add_signal_handler(signal.SIGTERM, stop_all)

        with self.binlog:
            logger.info("Binlog open")
            try:
                futures = [self.log_stats(),
                           self.put_in_binlog(),
                           server.wait_closed()]
                done, not_done = loop.run_until_complete(
                    asyncio.wait(futures,
                                 return_when=asyncio.FIRST_EXCEPTION))
            except:
                logger.exception("Server exception.")
                sys.exit(1)
            else:
                for e in done:
                    try:
                        e.result()
                    except:
                        logger.exception("Coroutine exception.")

                if done:
                    sys.exit(1)
            finally:
                loop.close()
                try:
                    os.unlink(self.uds_path)
                except IOError:
                    pass

        logger.info("Binlog closed")
