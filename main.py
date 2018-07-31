import os
import sys
import signal
import pickle
import asyncio

from control_manager import ControlManager
from control_server import ControlServer
from torrent_manager import Torrent
from models import TorrentInfo, generate_peer_id

DOWNLOAD_DIR = 'downloads'
STATE_FILE = 'state.bin'


async def handle_download(manager):
    try:
        await manager.download()
    finally:
        await manager.stop()


def signal_handler(torrent, task, loop):
    task.cancel()
    stop_task = asyncio.ensure_future(torrent.stop())
    stop_task.add_done_callback(lambda future: loop.stop())


def main():
    loop = asyncio.get_event_loop()
    control = ControlManager()
    loop.run_until_complete(control.start())
    if os.path.isfile(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
        with open(STATE_FILE, 'rb') as file:
            # torrent_info = pickle.load(file)
            control.load(file)
        print('state recovered')
    else:
        for arg in sys.argv[1:]:
            torrent_info = TorrentInfo.get_info(arg, download_dir = DOWNLOAD_DIR)
            control.add(torrent_info)
        print('new torrent has been successfully added')

    control_server = ControlServer(control)
    loop.run_until_complete(control_server.start())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        stop_task = asyncio.ensure_future(asyncio.wait([control_server.stop(), control.stop()]))
        stop_task.add_done_callback(lambda fut: loop.stop())
        loop.run_forever()
    finally:
        with open(STATE_FILE, 'wb') as file:
            control.dump(file)
        print('torrent state has been successfully saved')
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
