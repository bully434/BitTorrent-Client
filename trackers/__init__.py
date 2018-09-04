from urllib.parse import urlparse

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
    return client_class(parsed_announce_url, download_info, peer_id)
