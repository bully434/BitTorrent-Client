import os
import re
import sys
import asyncio
import argparse
import formatter

from functools import partial
from controllers.client import ControlClient
from controllers.manager import ControlManager
from controllers.server import ControlServer
from models import TorrentInfo

DOWNLOAD_DIR = 'downloads'
STATE_FILE = 'state.bin'
PATH_SPLIT_REGEX = re.compile(r'\\')


DEFAULT_DOWNLOAD_DIR = 'downloads'


def run_daemon(args):
    loop = asyncio.get_event_loop()
    control = ControlManager()
    loop.run_until_complete(control.start())
    if os.path.isfile(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
        with open(STATE_FILE, 'rb') as file:
            control.load(file)
        print('state recovered')

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


def show_torrent_handler(args):
    torrent_info = TorrentInfo.get_info(args.filename, download_dir=None)
    content = formatter.join(
        formatter.title_format(torrent_info) + formatter.content_format(torrent_info))
    print(content, end='')


async def delegate_to_control(action):
    client = ControlClient()
    await client.connect()
    try:
        return await client.execute(action)
    finally:
        client.close()


async def add(args):
    torrents = [TorrentInfo.get_info(filename, download_dir=args.directory) for filename in args.filenames]
    if args.include:
        paths = args.include
        mode = 'whitelist'
    elif args.exclude:
        paths = args.exclude
        mode = 'blacklist'
    else:
        paths = None
        mode = None
    if mode is not None:
        if len(torrents) > 1:
            raise ValueError("Can't handle --include and --exclude when several files are added")
        torrent_info = torrents[0]
        paths = [PATH_SPLIT_REGEX.split(path) for path in paths]
        torrent_info.download_info.select_files(paths, mode)
    async with ControlClient() as client:
        for info in torrents:
            await client.execute(partial(ControlManager.add, torrent_info=info))


async def control_action_handler(args):
    action = getattr(ControlManager, args.action)
    torrents = [TorrentInfo.get_info(filename, download_dir=None) for filename in args.filenames]
    for info in torrents:
        await delegate_to_control(partial(action, info_hash=info.download_info.info_hash))


def status_server_handler(manager):
    torrents = list(manager.get_torrents())
    if not torrents:
        return 'No torrents have been added'

    torrents.sort(key=lambda info: info.download_info.suggested_name)
    return '\n'.join(
        formatter.join(
            formatter.title_format(info) + formatter.status_format(info))
        for info in torrents).rstrip()


async def status_handler(args):
    status = await delegate_to_control(status_server_handler)
    print(status)


def main():
    parser = argparse.ArgumentParser(description='BitTorrent client')
    subparsers = parser.add_subparsers(description='Specify an action before "--help" to show parameters for it.',
                                       metavar='ACTION', dest='action')

    loop = asyncio.get_event_loop()

    subparser = subparsers.add_parser('start', help='start a server')
    subparser.set_defaults(func=run_daemon)

    subparser = subparsers.add_parser('show', help="Show torrent content")
    subparser.add_argument('filename', help='Torrent file')
    subparser.set_defaults(func=show_torrent_handler)

    subparser = subparsers.add_parser('add', help='Add new torrent')
    subparser.add_argument('filenames', nargs='+', help='Torrent file names')

    subparser.add_argument('-d', '--directory', default=DEFAULT_DOWNLOAD_DIR, help='Download directory')

    group = subparser.add_mutually_exclusive_group()
    group.add_argument('--include', action='append',
                       help='Download files and dirs marked with "--include"')
    group.add_argument('--exclude', action='append',
                       help='Download files and dirs except of marked with "--exclude"')

    subparser.set_defaults(func=lambda args: loop.run_until_complete(add(args)))

    control_commands = ['pause', 'resume', 'remove', 'priority']
    for command in control_commands:
        subparser = subparsers.add_parser(command, help='{} torrent'.format(command.capitalize()))
        subparser.add_argument('filenames', nargs='*' if command != 'remove' else '+', help='Torrent file names')
        subparser.set_defaults(func=lambda args: loop.run_until_complete(control_action_handler(args)))
    subparser = subparsers.add_parser('status', help='Show status')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(status_handler(args)))

    try:
        arguments = parser.parse_args()
        arguments.func(arguments)
    except (ValueError, RuntimeError) as e:
        print('Error: {}'.format(e), file=sys.stderr)
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
