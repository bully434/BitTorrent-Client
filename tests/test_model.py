import unittest
import sys
import asyncio
import os
from functools import partial

import bencoder
from torrent_manager import Torrent
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir))
from models import TorrentInfo, DownloadInfo
from file_structure import FileStructure
from controllers.manager import ControlManager
from controllers.client import ControlClient
from controllers.server import ControlServer


class MyTestCase(unittest.TestCase):
    def test_class(self):
        file = 'tests/test_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        print(torrent_info.download_info)
        announce = list(['http://t.bitlove.org/announce'])
        announce_list = list()
        announce_list.append(announce)
        dictionary = bencoder.decode(file)
        download_info = DownloadInfo.get_download_info(dictionary[b'info'])
        self.assertEqual(torrent_info.announce_list, announce_list)
        self.assertEqual(torrent_info.download_info.info_hash, download_info.info_hash)

    def test_file_structure(self):
        file = 'tests/test_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        pieces = torrent_info.download_info.pieces
        piece = pieces[0]
        piece.reset_content()
        piece.reset_run_state()
        piece.are_all_blocks_downloaded()
        piece.mark_as_downloaded()
        file_structure = FileStructure('download', torrent_info.download_info)
        self.assertEqual(file_structure.download_info.files[0].length, 19211729)
        self.assertEqual(file_structure.download_info.suggested_name, "bl001-introduction.webm")
        loop = asyncio.get_event_loop()
        file_structure.iterate_files(10, 10)
        loop.run_until_complete(file_structure.read(10,10))
        loop.run_until_complete(file_structure.write(10, b'hello'))
        file_structure.close()

    def test_client(self):
        file = 'tests/udp_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')

        loop = asyncio.get_event_loop()
        control_manager = ControlManager()
        self.assertEqual(control_manager.torrents, {})

        control_client = ControlClient()
        server = ControlServer(control_client)
        self.assertEqual(server.HOST, '127.0.0.1')
        loop.run_until_complete(server.start())
        client = ControlClient()

        server = control_manager.server
        _reader, _writer = loop.run_until_complete(asyncio.open_connection(host=ControlServer.HOST, port=6995))

        loop.run_until_complete(client.__aenter__())
        loop.run_until_complete(client.connect())
        loop.run_until_complete(client.execute(partial(control_manager.add, torrent_info)))
        loop.run_until_complete(client.__aexit__("TypeError", "123", "123"))
        client.close()
        loop.run_until_complete(server.accept(_reader, _writer))

    def test_download(self):
        file = 'tests/udp_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        info_hash_to_test = torrent_info.download_info.info_hash
        loop = asyncio.get_event_loop()
        control_manager = ControlManager()
        control_manager.add(torrent_info)
        loop.run_until_complete(control_manager.pause(torrent_info.download_info.info_hash))
        loop.run_until_complete(control_manager.priority(torrent_info.download_info.info_hash))
        control_manager.resume(torrent_info.download_info.info_hash)
        self.assertEqual(info_hash_to_test, torrent_info.download_info.info_hash)
        with open("test.bin", 'wb') as file:
            control_manager.dump(file)
        loop.run_until_complete(control_manager.remove(torrent_info.download_info.info_hash))
        with open("test.bin", 'rb') as file:
            control_manager.load(file)
        loop.run_until_complete(control_manager.start())
        loop.run_until_complete(control_manager.stop())

    @staticmethod
    def test_peer_tcp():
        file = 'tests/short.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        loop = asyncio.get_event_loop()
        control_manager = ControlManager()
        control_manager.add(torrent_info)
        loop.run_until_complete(control_manager.start())
        loop.run_until_complete(control_manager.stop())
        server = control_manager.server
        torrent = Torrent(torrent_info, server.peer_id, server.port)
