import unittest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir))
from models import TorrentInfo
import main


class MyTestCase(unittest.TestCase):
    def test_control_client(self):
        file = 'tests/test_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        main.format_torrent_info(torrent_info)
        pass


