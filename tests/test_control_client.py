
import unittest
import sys
import os
import torrent_formatter

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir))
from models import TorrentInfo


class MyTestCase(unittest.TestCase):

    def test_control_client(self):
        file = 'tests/test_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        pass

    def test_format(self):
        file = 'tests/test_torrent.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        size = torrent_formatter.humanize_size(torrent_info.download_info.total_size)
        self.assertEqual(size, '18.3 Mb')
        torrent_formatter.join(["123", "456"])
        content = torrent_formatter.content_format(torrent_info)
        self.assertEqual(content, None)
        status = torrent_formatter.status_format(torrent_info)
        self.assertEqual(status, status)
        title = torrent_formatter.title_format(torrent_info)
        self.assertEqual(title, title)
        big_size = torrent_formatter.humanize_size(1000000000)
        self.assertEqual(big_size, '953.7 Mb')
        speed = torrent_formatter.humanize_speed(1000)
        self.assertEqual(speed, '1000 bytes/s')
        file = 'tests/udp.torrent'
        torrent_info = TorrentInfo.get_info(file, download_dir='downloads')
        content = torrent_formatter.content_format(torrent_info)
        self.assertEqual(content, content)
        status = torrent_formatter.status_format(torrent_info)
        self.assertEqual(status, status)
        title = torrent_formatter.title_format(torrent_info)
        self.assertEqual(title, title)


