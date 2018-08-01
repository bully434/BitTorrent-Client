import os
import sys
import asyncio
import argparse


from functools import partial
from control_client import ControlClient
from control_manager import ControlManager
from control_server import ControlServer
from models import TorrentInfo, DownloadInfo

DOWNLOAD_DIR = 'downloads'
STATE_FILE = 'state.bin'

BYTES_PER_MIB = 2 ** 20


def humanize_size(size: int) -> str:
    return '{:.1f} Mb'.format(size / BYTES_PER_MIB)


def humanize_speed(speed: int) -> str:
    return '{:.1f} Mb/s'.format(speed / BYTES_PER_MIB)


def format_torrent_info(torrent_info):
    download_info = torrent_info.download_info
    result = 'Name: {}\n'.format(download_info.suggested_name)
    result += 'ID: {}\n'.format(download_info.info_hash.hex())

    if torrent_info.paused:
        state = 'Paused'
    elif download_info.complete:
        state = 'Uploading'
    else:
        state = 'Downloading'
    result += 'State: {}\n'.format(state)

    result += 'Download from: {}/{} peers\t'.format(download_info.downloading_peer_count, download_info.peer_count)
    result += 'Upload to: {}/{} peers\n'.format(download_info.uploading_peer_count, download_info.peer_count)
    result += 'Downloading speed: {}\t'.format(
        humanize_speed(download_info.download_speed) if download_info.download_speed is not None else 'unknown')
    result += 'Upload speed: {}\n'.format(
        humanize_speed(download_info.upload_speed) if download_info.download_speed is not None else 'unknown')

    last_piece_info = download_info.pieces[-1]
    downloaded_size = download_info.downloaded_piece_count * download_info.piece_length
    # if download_info.piece_downloaded[-1]:
    #     downloaded_size += last_piece_info - download_info.piece_length
    # selected_size = download_info.piece_selected.count() * download_info.piece_length
    # if download_info.piece_selected[-1]:
    #     selected_size += last_piece_info - download_info.piece_length
    if last_piece_info.downloaded:
        downloaded_size += last_piece_info.length - download_info.piece_length
    selected_piece_count = sum(1 for info in download_info.pieces if info.selected)
    selected_size = selected_piece_count * download_info.piece_length
    if last_piece_info.selected:
        selected_size += last_piece_info.length - download_info.piece_length

    result += 'Size: {}/{}\t'.format(humanize_size(downloaded_size), humanize_size(selected_size))
    if download_info.total_downloaded:
        ratio = download_info.total_uploaded / download_info.total_downloaded
    else:
        ratio = 0
    result += 'Ratio: {:.1f}\n'.format(ratio)

    progress = downloaded_size/selected_size
    progress_bar = ('#' * round(progress * 50)).ljust(50)
    result += 'Progress: {:5.1f}% [{}]\n'.format(progress*100, progress_bar)

    return result

def run_daemon(args):
    loop = asyncio.get_event_loop()
    control = ControlManager()
    loop.run_until_complete(control.start())
    if os.path.isfile(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
        with open(STATE_FILE, 'rb') as file:
            # torrent_info = pickle.load(file)
            control.load(file)
        print('state recovered')
    # else:
    #     for arg in sys.argv[1:]:
    #         torrent_info = TorrentInfo.get_info(arg, download_dir = DOWNLOAD_DIR)
    #         control.add(torrent_info)
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


async def delegate_to_control(action):
    client = ControlClient()
    await client.connect()
    try:
        return await client.execute(action)
    finally:
        client.close()


async def add(args):
    torrent_info = TorrentInfo.get_info(args.filename, download_dir = DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.add, torrent_info=torrent_info))


async def pause(args):
    torrent_info = TorrentInfo.get_info(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.pause, info_hash= torrent_info.download_info.info_hash))


async def resume(args):
    torrent_info = TorrentInfo.get_info(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.resume, info_hash= torrent_info.download_info.info_hash))


async def remove(args):
    torrent_info = TorrentInfo.get_info(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.remove, info_hash= torrent_info.download_info.info_hash))


async def status(args):
    torrent_list = await delegate_to_control(ControlManager.get_torrents)
    torrent_list.sort(key=lambda torrent_info: torrent_info.download_info.suggested_name)
    print('\n'.join(map(format_torrent_info, torrent_list)), end='')

def main():
    parser = argparse.ArgumentParser(description='BitTorrent client')
    subparsers = parser.add_subparsers(help='action')

    loop = asyncio.get_event_loop()

    parser_start = subparsers.add_parser('start', help='start a server')
    parser_start.set_defaults(func=run_daemon)

    subparser = subparsers.add_parser('add', help='add torrent')
    subparser.add_argument('filename', help='.torrent file')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(add(args)))

    subparser = subparsers.add_parser('pause', help='pause torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(pause(args)))

    subparser = subparsers.add_parser('resume', help='resume torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(resume(args)))

    subparser = subparsers.add_parser('remove', help='remove torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(remove(args)))

    subparser = subparsers.add_parser('status', help='show status')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(status(args)))

    try:
        arguments = parser.parse_args()
        arguments.func(arguments)
    except ValueError as e:
        print('Error: {}'.format(e), file=sys.stderr)
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
