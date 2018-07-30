import os
import bisect
import asyncio
import functools
from models import DownloadInfo


def delegate_executor(func):
    @functools.wraps(func)
    async def wrapper(self, *args, acquire_lock = True, **kwargs):
        if acquire_lock:
            await self.lock.acquire()
        try:
            return await self.loop.run_in_executor(None, functools.partial(func, self, *args, **kwargs))
        finally:
            if acquire_lock:
                self.lock.release()

    return wrapper


class FileStructure:
    def __init__(self, download_dir, download_info):
        self.download_info = download_info

        self.loop = asyncio.get_event_loop()
        self.lock = asyncio.Lock()
        self._descriptors = []
        self._offsets = []
        offset = 0

        try:
            for file in download_info.files:
                path = os.path.join(download_dir, download_info.suggested_name, *file.path)
                directory = os.path.dirname(path)
                if not os.path.isdir(directory):
                    os.makedirs(os.path.normpath(directory))
                if not os.path.isfile(path):
                    f = open(path, 'w')
                    f.close()

                f = open(path, 'r+b')
                f.truncate(file.length)

                self._descriptors.append(f)
                self._offsets.append(offset)
                offset += file.length

        except (OSError, IOError):
            for f in self._descriptors:
                f.close()
            raise

        self._offsets.append(offset)

    def iterate_files(self, offset, length):
        if offset < 0 or offset + length > self.download_info.total_size:
            raise IndexError('Data position out of range')

        index = bisect.bisect_right(self._offsets, offset) - 1

        while length != 0:
            file_start_offset = self._offsets[index]
            file_end_offset = self._offsets[index + 1]
            file_position = offset - file_start_offset
            bytes = min(file_end_offset - offset, length)
            descriptor = self._descriptors[index]
            yield descriptor, file_position, bytes

            offset += bytes
            length -= bytes
            index += 1

    @delegate_executor
    def read(self, offset, length):
        result = []
        for file, file_position, bytes in self.iterate_files(offset, length):
            file.seek(file_position)
            result.append(file.read(bytes))
        return b''.join(result)

    @delegate_executor
    def write(self, offset, data):
        for file, file_position, bytes in self.iterate_files(offset, len(data)):
            file.seek(file_position)
            file.write(data[:bytes])

            data = data[bytes:]

    @delegate_executor
    def flush(self, offset, length):
        for file, _, _ in self.iterate_files(offset, length):
            file.flush()

    def close(self):
        for file in self._descriptors:
            file.close()

