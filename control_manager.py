import asyncio
import copy
import pickle

from models import generate_peer_id, TorrentInfo
from peer_tcp_server import PeerTCPServer
from torrent_manager import Torrent


class ControlManager:
    def __init__(self):
        self.peer_id = generate_peer_id()
        self.torrents = {}
        self.torrent_managers = {}
        self.server = PeerTCPServer(self.peer_id, self.torrent_managers)
        self.torrent_managers_executors = {}

    async def start(self):
        await self.server.start()

    def start_torrent_manager(self, info_hash):
        torrent_info = self.torrents[info_hash]

        manager = Torrent(torrent_info, self.peer_id, self.server.port)
        self.torrent_managers[info_hash] = manager
        self.torrent_managers_executors[info_hash] = asyncio.ensure_future(manager.run())

    def add(self, torrent_info):
        info_hash = torrent_info.download_info.info_hash
        if info_hash in self.torrents:
            raise ValueError('This torrent is already added')

        self.torrents[info_hash] = torrent_info
        if not torrent_info.paused:
            self.start_torrent_manager(info_hash)

    def resume(self, info_hash):
        if info_hash not in self.torrents:
            raise ValueError('Torrent not found')
        torrent_info = self.torrents[info_hash]
        if not torrent_info.paused:
            raise ValueError('Torrent is already running')
        self.start_torrent_manager(info_hash)
        torrent_info.paused = False

    async def stop_torrent_manager(self, info_hash):
        manager_executor = self.torrent_managers_executors[info_hash]
        manager_executor.cancel()
        try:
            await manager_executor
        except asyncio.CancelledError:
            pass
        del self.torrent_managers_executors[info_hash]
        manager = self.torrent_managers[info_hash]
        await manager.stop()

        del self.torrent_managers[info_hash]

    async def remove(self, info_hash):
        if info_hash not in self.torrents:
            raise ValueError('Torrent not found')
        torrent_info = self.torrents[info_hash]

        if not torrent_info.paused:
            await self.stop_torrent_manager(info_hash)

        del self.torrents[info_hash]

    async def pause(self, info_hash):
        if info_hash not in self.torrents:
            raise ValueError('Torrent not found')
        torrent_info = self.torrents[info_hash]
        if torrent_info.paused:
            raise ValueError('Torrent is already paused')
        await self.stop_torrent_manager(info_hash)

        torrent_info.paused = True

    def dump(self, file):
        torrent_list = []
        for manager, torrent_info in self.torrents.items():
            torrent_info = copy.copy(torrent_info)
            torrent_info.download_info = copy.copy(torrent_info.download_info)
            torrent_info.download_info.reset_run_state()
            torrent_list.append(torrent_info)
        pickle.dump(torrent_list, file)
        print('state saved ({} torrents)'.format(len(torrent_list)))

    def load(self, file):
        torrent_list = pickle.load(file)
        for torrent_info in torrent_list:
            self.add(torrent_info)
        print('state recovered ({} torrents)'.format(len(torrent_list)))

    async def stop(self):
        for task in self.torrent_managers_executors.values():
            task.cancel()
        if self.torrent_managers_executors:
            await asyncio.wait(self.torrent_managers_executors.values())

        if self.torrent_managers:
            await asyncio.wait([manager.stop() for manager in self.torrent_managers.values()])
        await self.server.stop()
