import unittest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir))
from models import TorrentInfo, DownloadInfo
from file_structure import FileStructure
from control_manager import ControlManager
from control_client import ControlClient
from control_server import ControlServer
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
        pass

    def test_control(self):
        control_manager = ControlManager()
        control_client = ControlClient()
        control_server = ControlServer(control_client)
        pass