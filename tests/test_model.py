import unittest
import sys
import asyncio
import os


sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir))
from models import TorrentInfo, DownloadInfo
from file_structure import FileStructure
from controllers.manager import ControlManager
from controllers.client import ControlClient
from controllers.server import ControlServer
import bencoder

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
        file_structure = FileStructure('download', torrent_info.download_info)
        file_structure.close()
        pass

    def test_control(self):
        loop = asyncio.get_event_loop()
        control_manager = ControlManager()
        loop.run_until_complete(control_manager.start())
        loop.run_until_complete(control_manager.stop())
        control_client = ControlClient()
        loop.run_until_complete(control_client.connect())
        control_server = ControlServer(control_client)
        loop.run_until_complete(control_server.start())
        loop.run_until_complete(control_server.stop())
        pass