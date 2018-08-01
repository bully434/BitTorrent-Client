import time
import random
import asyncio
import hashlib
import itertools

from math import ceil
from typing import cast
from peer_tcp import PeerTCP, SeedError
from collections import deque, OrderedDict
from models import Peer, TorrentInfo, BlockRequestFuture
from file_structure import FileStructure
from tracker_http import TrackerGetRequest


def humanize_size(size):
    return '{:.1f} Mb'.format(size / 2 ** 20)


class NotEnoughPeers(RuntimeError):
    pass


class NoRequests(RuntimeError):
    pass


class PeerData:
    def __init__(self, client, connected_time):
        self.client = client
        self.connected_time = connected_time
        self.hanged_time = None
        self.queue_size = 0

    DOWNLOAD_REQUEST_QUEUE_SIZE = 10

    def is_free(self):
        return self.queue_size < PeerData.DOWNLOAD_REQUEST_QUEUE_SIZE

    def is_available(self):
        return self.is_free() and not self.client.peer_choking


class Torrent:
    def __init__(self, torrent_info, client_peer_id, server_port):
        self.torrent_info = torrent_info
        self.download_info = torrent_info.download_info
        self.download_info.reset_run_state()
        self.download_info.reset_stats()
        self.peer_id = client_peer_id
        self.server_port = server_port

        self.peer_data = {}
        self.client_executors = {}
        self.tasks = []
        self.request_executors = []
        self.executors_processed_requests = []
        # self.keep_alive_executor = None
        # self.announce_executor = None
        # self.upload_executor = None

        self.tracker_client = TrackerGetRequest(self.torrent_info, self.peer_id)

        self.pieces_to_download = None
        self.not_started_pieces = None
        self.download_start_time = None

        self.piece_block_queue = OrderedDict()

        self.endgame_mode = False

        self.tasks_waiting_for_peers = 0
        self.more_peers_requested = asyncio.Event()
        self.request_deque_relevant = asyncio.Event()

        self.file_structure = FileStructure(torrent_info.download_dir, torrent_info.download_info)

    DOWNLOAD_PEER_COUNT = 15
    DOWNLOAD_REQUEST_QUEUE_SIZE = 10
    TASKS_AWAITING_REQUEST = 15
    REQUEST_LENGTH = 2 ** 14
    HANG_PEER_TIME = 10
    PEER_LEAVING_ALONE_TIME_ENDGAME = 40

    NO_REQUESTS_SLEEP_TIME = 2
    NO_PEER_SLEEP_TIME = 3
    ANNOUNCE_FAILED_SLEEP_TIME = 3

    REQUEST_TIMEOUT = 6
    REQUEST_TIMEOUT_ENDGAME = 1

    MAX_CONNECT_PEERS = 30
    MAX_ACCEPT_PEERS = 55
    START_DURATION = 5
    NO_PEER_SLEEP_TIME_START = 1

    MIN_INTERVAL = 30
    KEEP_ALIVE_TIMEOUT = 2 * 60
    FAKE_SERVER_PORT = 6881

    async def execute_peer_client(self, peer, client, need_connect):
        try:
            if need_connect:
                await client.connect(self.download_info, self.file_structure)
            else:
                client.confirm_info_hash(self.download_info, self.file_structure)
            self.peer_data[peer] = PeerData(client, time.time())
            self.download_info.peer_count += 1
            await client.run()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("{} disconnected because of {}".format(peer, repr(e)))
        finally:
            if peer in self.peer_data:
                self.download_info.peer_count -= 1
                del self.peer_data[peer]

                for owners in self.download_info.piece_owners:
                    if peer in owners:
                        owners.remove(peer)
                if peer in self.download_info.peer_last_download:
                    del self.download_info.peer_last_downloaded[peer]
                if peer in self.download_info.peer_last_upload:
                    del self.download_info.peer_last_upload[peer]

            client.close()

    async def execute_keep_alive(self):
        while True:
            await asyncio.sleep(Torrent.KEEP_ALIVE_TIMEOUT)

            print('broadcasting keep-alives to {} alive peers'.format(len(self.peer_data)))

            for data in self.peer_data.values():
                data.client.send_keep_alive()

    def get_piece_position(self, index):
        piece_offset = index * self.download_info.piece_length
        piece_length = self.download_info.get_piece_length(index)
        return piece_offset, piece_length

    async def flush_piece(self, index):
        piece_offset, piece_length = self.get_piece_position(index)
        await self.file_structure.flush(piece_offset, piece_length)

    def get_peer_download_rate(self, peer):
        data = self.peer_data[peer]
        rate = data.client.downloaded
        rate += random.randint(1, 100)

        if data.hanged_time is not None and \
                time.time() - data.hanged_time <= Torrent.HANG_PEER_TIME:
            rate //= 100
        return rate

    def get_peer_upload_rate(self, peer):
        data = self.peer_data[peer]

        rate = data.client.downloaded
        if self.download_info.is_complete:
            rate += data.client.upliaded
        rate += random.randint(1, 100)

        return rate

    # async def select_piece(self):
    #     if not self.not_started_pieces:
    #         return False
    #     index = min(self.not_started_pieces, key=self.get_piece_order_rate)
    #     self.not_started_pieces.remove(index)
    #     await self.download_piece_start(index)
    #     return True

    def send_cancels(self, request):
        performers = request.prev_performer
        if request.performer is not None:
            performers.add(request.performer)
        source = request.result()
        for peer in performers - {source}:
            if peer in self.peer_data:
                self.peer_data[peer].client.send_request(request, cancel=True)

    async def download_piece_start(self, index):
        piece_length = self.download_info.get_piece_length(index)

        expected_blocks = self.download_info.piece_blocks_expected[index]
        request_deque = deque()
        for block_begin in range(0, piece_length, Torrent.REQUEST_LENGTH):
            block_end = min(block_begin + Torrent.REQUEST_LENGTH, piece_length)
            block_length = block_end - block_begin
            request = BlockRequestFuture(index, block_begin, block_length)
            request.add_done_callback(self.send_cancels)

            expected_blocks.add(request)
            request_deque.append(request)
        self.piece_block_queue[index] = request_deque

        self.download_info.interesting_pieces.add(index)
        piece_owners = self.download_info.piece_owners[index]
        for peer in piece_owners:
            self.peer_data[peer].client.am_interested = True

        # choking_owners = [peer for peer in piece_owners if self.peer_clients[peer].peer_choking]
        # if len(choking_owners) == len(piece_owners) and choking_owners:
        #     print('all piece owners are choking, waiting answer from am_interested')
        #     done, pending = await asyncio.wait([self.peer_clients[peer].drain() for peer in choking_owners],
        #                                        timeout=0.5)
        #     for future in done:
        #         if future.exception() is not None:
        #             print('drain failed with {}'.format(future.exception()))
        #     for future in pending:
        #         future.cancel()
        #     await asyncio.sleep(0.5)
        concurrent_peers_count = sum(1 for peer, data in self.peer_data.items() if data.queue_size)
        print('piece {} started (owned by {} peers, running {} peers)'
              .format(index, len(piece_owners),
                      concurrent_peers_count))

    async def download_piece_validate(self, index):
        piece_offset, piece_length = self.get_piece_position(index)
        data = await self.file_structure.read(piece_offset, piece_length)
        hash = hashlib.sha1(data).digest()
        if hash == self.download_info.piece_hashes[index]:
            await self.flush_piece(index)
            self.download_piece_finish(index)
            return
        for peer in self.download_info.piece_source[index]:
            self.download_info.increase_distust(peer)
            if self.download_info.is_banned(peer):
                print('Host {} banned'.format(peer.host))
                self.client_executors[peer].cancel()
        self.download_info.reset_piece(index)
        self.download_piece_start(index)
        print('piece {} is not valid, retrying'.format(index))

    def request_piece_blocks(self, count, index):
        if not count:
            return

        request_deque = self.piece_block_queue[index]
        performer = None
        performer_data = None
        yielded_count = 0
        while request_deque:
            request = request_deque[0]
            if request.done():
                request_deque.popleft()
                continue
            if performer is None or not performer_data.is_free():
                available_peers = {peer for peer in self.download_info.piece_owners[index]
                                   if self.peer_data[peer].is_available()}
                if not available_peers:
                    return
                performer = max(available_peers, key=self.get_peer_download_rate)
                performer_data = self.peer_data[performer]
            request_deque.popleft()
            performer_data.queue_size += 1
            request.performer = performer
            performer_data.client.send_request(request)
            yield request
            yielded_count += 1
            if yielded_count == count:
                return

    PIECE_COUNT_FOR_SELECTION = 10

    def select_new_piece(self, *, force):
        is_appropriate = PeerData.is_free if force else PeerData.is_available
        appropriate_peers = {peer for peer, data in self.peer_data.items() if is_appropriate(data)}

        if not appropriate_peers:
            return None

        piece_owners = self.download_info.piece_owners
        available_pieces = [index for index in self.not_started_pieces
                            if appropriate_peers & piece_owners[index]]
        if not available_pieces:
            return None

        available_pieces.sort(key=lambda index: len(piece_owners[index]))
        piece_count_for_selection = min(len(available_pieces), Torrent.PIECE_COUNT_FOR_SELECTION)
        return available_pieces[random.randint(0, piece_count_for_selection - 1)]

    PIECE_LENGTH = 2 ** 20
    REQ_PER_PIECE = ceil(PIECE_LENGTH / REQUEST_LENGTH)
    DESIRED_REQUEST = DOWNLOAD_PEER_COUNT * DOWNLOAD_REQUEST_QUEUE_SIZE
    DESIRED_PIEСE_STOCK = ceil(DESIRED_REQUEST / REQ_PER_PIECE)

    async def request_blocks(self, count):
        result = []
        consumed_pieces = []
        for index, request_deque in self.piece_block_queue.items():
            result += list(self.request_piece_blocks(count - len(result), index))
            if not request_deque:
                consumed_pieces.append(index)
            if len(result) == count:
                return result

        piece_stock = len(self.piece_block_queue) - len(consumed_pieces)
        piece_stock_small = (piece_stock < Torrent.DESIRED_PIEСE_STOCK)
        new_piece_index = self.select_new_piece(force=piece_stock_small)

        if new_piece_index is not None:
            self.not_started_pieces.remove(new_piece_index)
            await self.download_piece_start(new_piece_index)

            result += list(self.request_piece_blocks(count - len(result), new_piece_index))
            if not self.piece_block_queue[new_piece_index]:
                del self.piece_block_queue[new_piece_index]

        if not result:
            if not self.piece_block_queue and not self.not_started_pieces:
                raise NoRequests('No more undistributed requests')
            raise NotEnoughPeers('No peers to perform')
        return result

    def download_piece_finish(self, index):
        self.download_info.mark_piece_downloaded(index)

        self.download_info.interesting_pieces.remove(index)
        for peer in self.download_info.piece_owners[index]:
            client = self.peer_data[peer].client
            for index in self.download_info.interesting_pieces:
                if client.piece_owned[index]:
                    break
            else:
                client.am_interested = False
        for data in self.peer_data.values():
            data.client.send_have(index)
        print('piece {} finished'.format(index))
        progress = self.download_info.downloaded_piece_count / len(self.pieces_to_download)
        print('progress {:.1%} ({} / {} pieces)'.format(progress,
                                                        self.download_info.downloaded_piece_count,
                                                        len(self.pieces_to_download)))

    DOWNLOAD_PEER_ACTIVE = 2

    async def wait_more_peers(self):
        self.tasks_waiting_for_peers += 1
        download_peer_active = Torrent.DOWNLOAD_PEER_COUNT - self.tasks_waiting_for_peers
        if download_peer_active <= Torrent.DOWNLOAD_PEER_ACTIVE and \
                len(self.peer_data) < Torrent.MAX_CONNECT_PEERS:
            self.more_peers_requested.set()

        if time.time() - self.download_start_time <= Torrent.START_DURATION:
            sleep_time = Torrent.NO_PEER_SLEEP_TIME_START
        else:
            sleep_time = Torrent.NO_PEER_SLEEP_TIME
        await asyncio.sleep(sleep_time)
        self.tasks_waiting_for_peers -= 1

    async def wait_more_requests(self):
        if not self.endgame_mode:
            not_finished_pieces = [i for i in self.pieces_to_download
                                   if not self.download_info.piece_downloaded[i]]
            print('starting endgame mode (pieces left: {})'.format(', '.join(map(str, not_finished_pieces))))

            self.endgame_mode = True
        await self.request_deque_relevant.wait()

    async def execute_block_requests(self, processed_requests):
        while True:
            try:
                free_place_count = Torrent.DOWNLOAD_REQUEST_QUEUE_SIZE - len(processed_requests)
                processed_requests += await self.request_blocks(free_place_count)
            except NotEnoughPeers:
                if not processed_requests:
                    await self.wait_more_peers()
                    continue

            except NoRequests:
                if not processed_requests:
                    if not any(self.executors_processed_requests):
                        self.request_deque_relevant.set()
                        return
                    await self.wait_more_requests()
                    continue

            if self.endgame_mode:
                request_timeout = Torrent.REQUEST_TIMEOUT_ENDGAME
            else:
                request_timeout = Torrent.REQUEST_TIMEOUT

            requests_done, requests_pending = await asyncio.wait(processed_requests,
                                                                 return_when=asyncio.FIRST_COMPLETED,
                                                                 timeout=request_timeout)

            if len(requests_pending) < len(processed_requests):
                piece_validating = self.download_info.piece_validating
                for request in requests_done:
                    if request.performer in self.peer_data:
                        self.peer_data[request.performer].queue_size -= 1

                    piece_index = request.piece_index
                    if not piece_validating[piece_index] and \
                            not self.download_info.piece_downloaded[piece_index] and \
                            not self.download_info.piece_blocks_expected[piece_index]:
                        piece_validating[piece_index] = True
                        await self.download_piece_validate(request.piece_index)
                        piece_validating[piece_index] = False
                processed_requests.clear()
                processed_requests += list(requests_pending)
            else:
                hanged_peers = {request.performer for request in requests_pending} & set(self.peer_data.keys())
                current_time = time.time()
                for peer in hanged_peers:
                    self.peer_data[peer].hanged_time = current_time

                if hanged_peers:
                    print('peer {} hanged'.format(','.join(map(str, hanged_peers))))

                for request in requests_pending:
                    if request.performer in self.peer_data:
                        self.peer_data[request.performer].queue_size -= 1
                        request.prev_performer.add(request.performer)
                    request.performer = None
                    self.piece_block_queue.setdefault(request.piece_index, deque()).append(request)

                processed_requests.clear()
                self.request_deque_relevant.set()
                self.request_deque_relevant.clear()

    def connect_to_peers(self, peers, force):
        peers = list({peer for peer in peers
                      if peer not in self.peer_data })
                      # and not self.download_info.is_banned(peer)})
        if force:
            max_peers_count = Torrent.MAX_ACCEPT_PEERS
        else:
            max_peers_count = Torrent.MAX_CONNECT_PEERS
        connecting_peers_count = max(max_peers_count - len(self.peer_data), 0)
        print('connecting to {} new peers'.format(min(len(peers), connecting_peers_count)))

        for peer in peers[:connecting_peers_count]:
            client = PeerTCP(self.peer_id, peer)
            self.client_executors[peer] = asyncio.ensure_future(
                self.execute_peer_client(peer, client, need_connect=True))

    def accept_client(self, peer, client):
        if len(self.peer_data) > Torrent.MAX_ACCEPT_PEERS:
            client.close()
            return
        print('accepted connection from {}'.format(peer))
        self.client_executors[peer] = asyncio.ensure_future(
            self.execute_peer_client(peer, client, need_connect=False))

    async def try_to_announce(self, event):
        try:
            server_port = self.server_port if self.server_port is not None else Torrent.FAKE_SERVER_PORT
            await self.tracker_client.announce(server_port, event)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print('exception on announce: {}'.format(e))
            return False

    async def execute_regular_announcement(self):
        try:
            while True:
                if self.tracker_client.min_interval is not None:
                    min_interval = self.tracker_client.min_interval
                else:
                    min_interval = min(Torrent.MIN_INTERVAL, self.tracker_client.interval)
                await asyncio.sleep(min_interval)
                default_interval = self.tracker_client.interval
                try:
                    await asyncio.wait_for(self.more_peers_requested.wait(),
                                           default_interval - min_interval)
                    more_peers = True
                    self.more_peers_requested.clear()
                except asyncio.TimeoutError:
                    more_peers = False
                await self.try_to_announce(None)
                self.connect_to_peers(self.tracker_client.peers, more_peers)
        finally:
            await self.try_to_announce('stopped')

    @property
    def download_complete(self):
        return self.download_info.downloaded_piece_count == len(self.pieces_to_download)

    async def download(self, pieces=None):
        self.download_start_time = time.time()
        if pieces is None:
            pieces = range(self.download_info.piece_count)
        self.pieces_to_download = pieces
        self.not_started_pieces = [index for index in self.pieces_to_download
                                   if not self.download_info.piece_downloaded[index]]

        if not self.not_started_pieces:
            return

        random.shuffle(self.not_started_pieces)

        for _ in range(Torrent.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self.executors_processed_requests.append(processed_requests)
            self.request_executors.append(asyncio.ensure_future(self.execute_block_requests(processed_requests)))

        await asyncio.wait(self.request_executors)

        assert self.download_info.is_complete()
        await self.try_to_announce('completed')
        print('download complete')
        for peer, data in self.peer_data.items():
            if data.client.is_seed():
                self.client_executors[peer].cancel()

    CHOKING_TIME = 10
    UPLOAD_PEER_COUNT = 4
    ITER_PER_UNCHOKING = 3
    CONN_RECENT_THRESHOLD = 60
    CONN_RECENT_COEFF = 3

    def select_optimistically_unchoked(self, peers):
        current_time = time.time()
        peers_connected_recently = []
        peers_remaining = []
        for peer in peers:
            if current_time - self.peer_data[peer].connected_time <= Torrent.CONN_RECENT_THRESHOLD:
                peers_connected_recently.append(peer)
            else:
                peers_remaining.append(peer)

        max_index = len(peers_remaining) + Torrent.CONN_RECENT_COEFF * len(peers_connected_recently) - 1
        index = random.randint(0, max_index)
        if index < len(peers_remaining):
            return peers_remaining[index]
        return peers_connected_recently[(index - len(peers_remaining)) % len(peers_connected_recently)]

    async def execute_uploading(self):
        peers_unchoked_previous = ()
        optimistically_unchoked = None
        for e in itertools.count():
            peers_alive = list(sorted(self.peer_data.keys(), key=self.get_peer_upload_rate, reverse=True))
            peers_unchoked_current = set()
            interested_count = 0

            if Torrent.UPLOAD_PEER_COUNT:
                if e % Torrent.ITER_PER_UNCHOKING == 0:
                    if peers_alive:
                        optimistically_unchoked = self.select_optimistically_unchoked(peers_alive)
                    else:
                        optimistically_unchoked = None
                if optimistically_unchoked is not None and optimistically_unchoked in self.peer_data:
                    peers_unchoked_current.add(optimistically_unchoked)
                    if self.peer_data[optimistically_unchoked].client.peer_interested:
                        interested_count += 1

            for peer in peers_alive:
                if interested_count == Torrent.UPLOAD_PEER_COUNT:
                    break
                if self.peer_data[peer].client.peer_interested:
                    interested_count += 1
                peers_unchoked_current.add(peer)

            for peer in set(peers_unchoked_previous) - peers_unchoked_current:
                if peer in self.peer_data:
                    self.peer_data[peer].client.am_choking = True

            for peer in peers_unchoked_current:
                self.peer_data[peer].client.am_choking = False
            print('{} peers unchoked (total uploaded = {})'
                  .format(len(peers_unchoked_current),
                          humanize_size(self.download_info.total_uploaded)))

            await asyncio.sleep(Torrent.CHOKING_TIME)

            peers_unchoked_previous = peers_unchoked_current

    SPEED_MEASUREMENT_PERIOD = 10
    SPEED_UPDATE_TIMEOUT = 2

    async def execute_speed_measure(self):
        max_queue_length = Torrent.SPEED_MEASUREMENT_PERIOD // Torrent.SPEED_UPDATE_TIMEOUT
        downloaded_queue = deque()
        uploaded_queue = deque()

        while True:
            downloaded_queue.append(self.download_info.downloaded_per_session)
            uploaded_queue.append(self.download_info.uploaded_per_session)

            if len(downloaded_queue) > 1:
                periods = (len(downloaded_queue) - 1) * Torrent.SPEED_UPDATE_TIMEOUT
                downloaded_per_period = downloaded_queue[-1] - downloaded_queue[0]
                uploaded_per_period = uploaded_queue[-1] - uploaded_queue[0]
                self.download_info.download_speed = downloaded_per_period / periods
                self.download_info.upload_speed = uploaded_per_period/ periods

            if len(downloaded_queue) > max_queue_length:
                downloaded_queue.popleft()
                uploaded_queue.popleft()

            await asyncio.sleep(Torrent.SPEED_UPDATE_TIMEOUT)
            return downloaded_queue

    async def run(self):
        while not await self.try_to_announce('started'):
            await asyncio.sleep(Torrent.ANNOUNCE_FAILED_SLEEP_TIME)
        self.connect_to_peers(self.tracker_client.peers, False)
        self.tasks += [
            asyncio.ensure_future(self.execute_keep_alive()),
            asyncio.ensure_future(self.execute_regular_announcement()),
            asyncio.ensure_future(self.execute_uploading()),
            self.execute_speed_measure()
        ]
        await self.download()

    async def stop(self):

        executors = self.request_executors + self.tasks + list(self.client_executors.values())

        executors = [task for task in executors if task is not None]

        for task in executors:
            task.cancel()

        if executors:
            await asyncio.wait(executors)

        self.request_executors.clear()
        self.executors_processed_requests.clear()
        self.tasks.clear()
        self.client_executors.clear()

        self.file_structure.close()
