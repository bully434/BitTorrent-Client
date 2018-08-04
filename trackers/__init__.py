from urllib.parse import urlparse

from models import TorrentInfo
from trackers.base import *
from trackers.http import *
from trackers.udp import *


def create_tracker_client(announce_url, download_info, peer_id):
    parsed_announce_url = urlparse(announce_url)
    scheme = parsed_announce_url.scheme
    protocols = {
        'http': HTTPTracker,
        'https': HTTPTracker,
        'udp': UDPTracker,
    }
    if scheme not in protocols:
        raise ValueError('announce url uses unknown protocol {}'.format(scheme))
    client_class = protocols[scheme]
    # print('url {}'.format(parsed_announce_url))
    # print('info {}'.format(download_info))
    # print('ID {}'.format(peer_id))
    return client_class(parsed_announce_url, download_info, peer_id)
