import contextlib
import os
import signal

from .common import FileDownloader
from ..dependencies import websockets


class FFmpegSinkFD(FileDownloader):
    """ A sink to ffmpeg for downloading fragments in any form """

    def real_download(self, filename, info_dict):
        info_copy = info_dict.copy()
        info_copy['url'] = '-'

        async def call_conn(proc, stdin):
            try:
                await self.real_connection(stdin, info_dict)
            except OSError:
                pass
            finally:
                with contextlib.suppress(OSError):
                    stdin.flush()
                    stdin.close()
                os.kill(os.getpid(), signal.SIGINT)

        return

    async def real_connection(self, sink, info_dict):
        """ Override this in subclasses """
        raise NotImplementedError('This method must be implemented by subclasses')


class WebSocketFragmentFD(FFmpegSinkFD):
    async def real_connection(self, sink, info_dict):
        async with websockets.connect(info_dict['url'], extra_headers=info_dict.get('http_headers', {})) as ws:
            while True:
                recv = await ws.recv()
                if isinstance(recv, str):
                    recv = recv.encode('utf8')
                sink.write(recv)
