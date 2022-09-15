import asyncio
import random
from xmlrpc.client import MAXINT
from api import APIHandler
import socket
import hashlib

GOSSIP_FORWARD = 3000

GOSSIP_PUSHVIEW = 504
GOSSIP_PUSHANSWER = 505
GOSSIP_PULLREQUEST = 506
GOSSIP_PULLVIEW = 507
GOSSIP_CHALLANGE = 508


class View:
    """
    Class for storing peers. Essentially a thread safe list with some extra functions.

        Attributes:
            view (list): list of (ip, port) for neighboring peers
            min_n (int): Minimal length. If the view is smaller, it's in a critical state.
            max_n (int): Maximal length. The view cannot be longer.
            view_lock (asyncio.Lock): lock for parallel access to the list
    """

    def __init__(self, min_n=2, degree=5, max_n=15):
        """
        Constructor for an empty view.

            Parameters:
                min_n (int): Minimal length. If the view is smaller, it's in a critical state.
                max_n (int): Maximal length. The view cannot be longer.
        """
        self.view = []
        self.max_n = max_n
        self.degree = degree
        self.min_n = min_n
        self.view_lock = asyncio.Lock()

    async def get_view(self):
        """Returns the view as a list of tuples (ip (str), port (int))"""
        async with self.view_lock:
            view = self.view.copy()
        return view

    async def is_in_view(self, peer):
        """Return True if peer is in the view."""
        async with self.view_lock:
            res = peer in self.view
        return res

    async def length(self):
        """Return the current number of peers in the view."""
        async with self.view_lock:
            n = len(self.view)
        return n

    async def is_empty(self):
        """Return True if the view is empty."""
        return await self.length() == 0

    async def is_full(self):
        """Return True if the list is full ."""
        return await self.length() == self.max_n

    async def delete(self, peer):
        """Remove peer (ip, port) from the view."""
        async with self.view_lock:
            self.view.remove(peer)

    async def delete_by_index(self, index):
        """Remove the peer at the given index from the view."""
        async with self.view_lock:
            del self.view[index]

    async def reset_view(self):
        """Remove all peers from the view."""
        async with self.view_lock:
            self.view = []

    async def too_small(self):
        async with self.view_lock:
            condition = len(self.view) < self.min_n
        return condition

    async def try_update(self, peer):
        """
        Add peer to the view. Return false if it is already in the view or the view is full.

        Parameter peer (str, int): Peer that should be added to the view
        Return (bool): True if the update was successful
        """
        peer = (socket.gethostbyname(peer[0]), peer[1])  # resolve dns name if necessary
        if await self.is_full():
            return False
        async with self.view_lock:
            if peer in self.view:
                return False
            self.view.append(peer)
        return True

    async def force_update(self, peer):
        """Add a peer to the view, even if it is already added"""
        peer = (socket.gethostbyname(peer[0]), peer[1])
        if await self.is_full():
            await self.delete_by_index(0)
        async with self.view_lock:
            self.view.append(peer)


class GossipPeer:
    """
    Class that represents a gossip peer. Handles internal messages and maintains the view.

    Attributes:
        verbose (bool): print debug messages if True
        address (str): ip address on which other peers can contact this peer
        port (int): port on which other peers can contact this peer
        shutdown_event (asyncio.Event): event that is set if the peer wants to shut down
        pull_done (asyncio.Event): event that is set when a pull request has been answered
        auto_gossip (bool): To automatic pushes and pulls only if this is True
        apihandler (APIHandler): Manages client communication
        view (View): stores the view
        listener (asyncio coroutine): server that listens for incoming messages
    """

    def __init__(self, my_address="localhost", my_port=9001, peer_ip=0, peer_port=0, api_ip = "localhost", api_port=7001, auto_gossip=True,
                 verbose=False, degree = 5, cache_size = 15):
        """
        Constructor for a GossipPeer.

        Creates and starts all needed coroutines for a GossipPeer, but does not start them yet.

        Parameters:
            my_address (str): ip-address or domain name for the internal communication of the peer
            my_port (int): port for the internal communication of the peer
            peer_ip (str): optional ip-address for a bootstrap-peer
            peer_port (int): optional port for a bootstrap-peer
            api_ip (str): IP-adress of API, defaults to localhost
            api_port (int): change the API-port for testing multiple peers, defaults to 7001
            auto_gossip (bool): switch on and off automatic pushes / pulls, defaults to True
            verbose (bool): switch on and off debug printing, defaults to False
            degree (int):  number of peers the current peer has to exchange information with
            cache_size (int): maximum number of data items to be held as part of the peer's knowledge base.
        """
        self.verbose = verbose
        self.address = my_address
        self.address = socket.gethostbyname(my_address)
        self.port = my_port
        self.shutdown_event = asyncio.Event()
        self.auto_gossip = auto_gossip
        self.pull_done = asyncio.Event()
        self.view_too_small = asyncio.Event()
        event_loop = asyncio.get_event_loop()
        api_ip = socket.gethostbyname(api_ip)
        # initialize API handler
        self.apihandler = APIHandler(self.forward_message, api_ip, api_port, verbose=verbose)
        # initialize view
        self.view = View(degree=degree, max_n=cache_size)
        if peer_ip != 0 and peer_port != 0:
            event_loop.create_task(self.view.try_update((peer_ip, peer_port)))
        # initialize listener
        self.listener = asyncio.start_server(self.read_peer_message, host=my_address, port=my_port,
                                             family=socket.AddressFamily.AF_INET)
        self.debug_print("Initialized peer")

    async def run(self):
        """Start all threads of the GossipPeer"""
        event_loop = asyncio.get_event_loop()
        event_loop.create_task(self.apihandler.start())  # start server
        event_loop.create_task(self.listener)  # start listener
        if self.auto_gossip:
            event_loop.create_task(self.maintain_view())  # start maintain view thread

    async def shutdown(self):
        """
        Shut the peer down. Wait for all threads of the peer to finish.

        Shuts down the APIHandler and clears the View. The peer cannot be started again afterwards.
        """
        self.debug_print("Shutting peer down...")
        self.shutdown_event.set()  # this wakes up the maintain view thread to shut down
        self.listener.close()
        await self.apihandler.shutdown()
        await self.view.reset_view()
        self.shutdown_event.clear()
        self.debug_print("Shutdown complete")

    def debug_print(self, *s):
        """Print what the peer does for debugging."""
        if self.verbose:
            msg = "[Peer " + str(self.address) + ':' + str(self.port) + ']: '
            print(msg, *s)

    def start_auto_gossip(self):
        """Start automatic pushes/pulls for a peer that is already running."""
        self.auto_gossip = True
        event_loop = asyncio.get_event_loop()
        event_loop.create_task(self.maintain_view())

    async def alarm_clock(self, timeout):
        """
        Returns when the view is too small, shutdown happens or the time runs out.

        Parameters:
            timeout (float): minimal seconds to sleep before returning

        Returns:
            (bool): False if a shutdown event happened, True otherwise
        """
        # returns when either timeout, shutdown event or view too small event
        event_loop = asyncio.get_event_loop()
        wait_for_shutdown = event_loop.create_task(self.shutdown_event.wait())  # start task that waits for shutdown
        view_too_small = event_loop.create_task(self.view_too_small.wait())  # start task that waits for view too small
        coros = [wait_for_shutdown, view_too_small]
        # wait until one of the tasks is completed
        done, pending = await asyncio.wait(coros, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:  # cancel the other tasks
            task.cancel()
        if view_too_small in done:
            self.view_too_small.clear()
        if wait_for_shutdown in done:
            return False
        return True

    async def maintain_view(self):
        """Maintain a peers view by doing regular pulls and emergency pushes."""
        self.debug_print("Starting auto gossip")
        random_number2 = random.randint(0, 10)
        while await self.alarm_clock(timeout=random_number2):  # returns false when shutdown
            random_number2 = random.randint(0, 10)  # wait random time to break symmetry
            view = await self.view.get_view()
            if len(view) >= self.view.min_n:  # do a pull
                random_number = random.randint(0, len(view) - 1)
                random_peer = view[random_number]
                self.pull_done.clear()
                await self.pull_request(*random_peer)
                try:  # wait max 20 seconds for the push to be answered
                    await asyncio.wait_for((asyncio.shield(self.pull_done.wait())), 20)
                except asyncio.exceptions.TimeoutError:  # push was not answered, we remove the bad neighbor and try again
                    self.debug_print("Removed peer", random_peer, "that was not answering a pull request")
                    await self.view.delete(random_peer)
                    continue
            else:  # do a push
                await self.push_view()  # view is too small, emergency push
                await asyncio.sleep(2)  # give peers a chance to answer before pushing again

    async def read_peer_message(self, r, w):
        """
        Read an incoming internal message.

        This function is called by the listener. It sends the challenge to the contacting peer, checks the message type
        and the length and calls the function to deal with this message type. Is also checks if the hash of the message
        is correct.

        Parameters:
            r (asyncio.StreamReader): for reading from the peer that contacted us
            w (asyncio.StreamWriter): for answering the peer that contacted us
        """
        gossip_challenge = int.to_bytes(GOSSIP_CHALLANGE, 2, "big")
        size = int.to_bytes(8, 2, "big")
        challenge = int.to_bytes(random.randint(0, MAXINT), 4, "big")
        msg = size + gossip_challenge + challenge
        w.write(msg)
        header = await r.read(4)  # read only header
        if header == b'':  # peer not alive anymore
            return
        size = int.from_bytes(header[:2], "big")
        msg = header + await r.read(size - 4)  # read rest of the message
        if len(msg) != size:
            print("Ignored message of invalid length")
            w.close()
            await w.wait_closed()
            return
        if hashlib.sha256(msg).hexdigest()[0:2] != "00":  # check hash
            print("Ignored message with invalid hash")
            w.close()
            await w.wait_closed()
            return
        msg_type = int.from_bytes(msg[2:4], "big")
        if msg_type == GOSSIP_PUSHVIEW and challenge == msg[12:16]:
            await self.handle_pushview(msg)
        elif msg_type == GOSSIP_PUSHANSWER and challenge == msg[4:8]:
            await self.handle_pushanswer(msg)
        elif msg_type == GOSSIP_PULLREQUEST and challenge == msg[12:16]:
            await self.handle_pullrequest(msg)
        elif msg_type == GOSSIP_PULLVIEW and challenge == msg[4:8]:
            await self.handle_pullview(msg)
        elif msg_type == GOSSIP_FORWARD and challenge == msg[8:12]:
            await self.handle_forward_message(msg)
        else:
            print("Message ignored due to bad format or unknown message type")
        w.close()
        await w.wait_closed()
        if await self.view.too_small():
            # wake up the view maintain thread to grow the view
            self.view_too_small.set()

    async def handle_pushview(self, msg):
        """
        Handle an incoming PUSHVIEW message. Add sender and content of the message to the view. Answer with PUSHANSWER.

        :param msg: PUSHVIEW message including header
        """
        size = int.from_bytes(msg[:2], "big")
        if size != len(msg):
            self.debug_print("PUSHVIEW with invalid size")
            return  # handle bad message
        ip_answer = ".".join(list(map(str, msg[4:8])))
        port_answer = int.from_bytes(msg[8:12], "big")
        if not await self.view.is_in_view((ip_answer, port_answer)):  # add the sender to the view
            await self.view.try_update((ip_answer, port_answer))
            self.debug_print("PUSHVIEW: Added", ip_answer, "on port", port_answer, "to the view")
        data = msg[20:size]
        address = b""
        i = 0
        view_copy = await self.view.get_view()
        while i < len(data):
            peer = int.to_bytes(data[i], 1, "big")
            if peer != b":":
                address += peer
            else:
                ip = address.decode()
                port = int.from_bytes(data[i + 1:i + 3], "big")
                if not await self.view.is_in_view((ip, port)):
                    await self.view.force_update((ip, port))
                    self.debug_print("Added", ip, "on port", port, "to the view")
                i = i + 3
                address = b""
            i = i + 1
        if (self.address, self.port) in view_copy:
            view_copy.remove((self.address, self.port))
        await self.push_answer(view_copy, ip_answer, port_answer)

    async def handle_pushanswer(self, msg):
        """
        Handle an incoming PUSHANSWER message. Add the content to the view.

        :param msg: PUSHANSWER message including header
        """
        size = int.from_bytes(msg[:2], "big")
        if size != len(msg):
            pass  # handle bad message
        peers = msg[12:size]
        address = b""
        i = 0
        while i < len(peers):
            peer = int.to_bytes(peers[i], 1, "big")
            if peer != b":":
                address += peer
            else:
                ip = address.decode()
                port = int.from_bytes(peers[i + 1:i + 3], "big")
                if not (await self.view.is_in_view((ip, port))):
                    await self.view.try_update((ip, port))
                    self.debug_print("PUSHANSWER, Added", ip, "on port", port, "to the view")
                i = i + 3
                address = b""
            i = i + 1

    async def push_view(self):
        """
        Send a PUSHVIEW to all neighbors.

        Encodes the view and finds the right nonce. Does not wait for an answer.
        """
        peers = await self.view.get_view()
        for i in range(0, len(peers)):
            view = b''
            for peer in peers:
                if not peer == peers[i]:
                    view += peer[0].encode() + b":" + int.to_bytes(peer[1], 2, "big") + b";"
            try:
                r, w = await asyncio.open_connection(peers[i][0], peers[i][1])
            except Exception as e:
                print(
                    "Failed to build a connection with node {0}:{1} during pushView creating due to an error: {2}".format(
                        peers[i][0], peers[i][1], e))
                await self.view.delete((peers[i][0], peers[i][1]))
                continue
            challenge_msg = b""
            while challenge_msg == b"":
                challenge_msg = await r.read(8)
            challenge = challenge_msg[4:8]
            ip = bytes(list(map(int, socket.gethostbyname(self.address).split("."))))
            port = int.to_bytes(self.port, 4, "big")
            size = int.to_bytes(20 + len(view), 2, "big")
            push_view = int.to_bytes(GOSSIP_PUSHVIEW, 2, "big")
            msg = size + push_view + ip + port + challenge
            msg = await self.find_nonsence(msg, view)
            self.debug_print("Sending PUSHVIEW to", peers[i][0], 'on port', peers[i][1])
            w.write(msg)
            w.close()
            await w.wait_closed()

    async def push_answer(self, view_copy, ip, port):
        """
        Send a PUSHANSWER to a peer.

        :param view_copy: encoding of the current view as string
        :param ip: address of the receiver
        :param port: port of the receiver
        """
        try:
            r, w = await asyncio.open_connection(ip, port)
        except Exception as e:
            print(
                "Failed to build a connection with node {0}:{1} during pushAnswer creating due to an error: {2}".format(
                    ip, port, e))
            await self.view.delete((ip, port))
            return
        challenge_msg = b""
        while challenge_msg == b"":
            challenge_msg = await r.read(8)
        challenge = challenge_msg[4:8]
        peers = b""
        for peer in view_copy:
            if not peer == (ip, port):
                peers += peer[0].encode() + b":" + int.to_bytes(peer[1], 2, "big") + b";"
        size = int.to_bytes(12 + len(peers), 2, "big")
        push_answer = int.to_bytes(GOSSIP_PUSHANSWER, 2, "big")
        msg = size + push_answer + challenge
        msg = await self.find_nonsence(msg, peers)
        self.debug_print("Sending PUSHANSWER to", ip, "from port", port)
        w.write(msg)

    async def handle_pullrequest(self, msg):
        """
        Handle a PULLREQUEST. Add the sender to the view. Answer with PULLVIEW.

        :param msg: PULLREQUEST message including header
        """
        size = int.from_bytes(msg[:2], "big")
        if size != len(msg):
            pass  # handle bad message
        ip_answer = ".".join(list(map(str, msg[4:8])))
        port_answer = int.from_bytes(msg[8:12], "big")
        if not (ip_answer, port_answer) in await self.view.get_view():
            await self.view.try_update((ip_answer, port_answer))
            self.debug_print("PULLREQUEST: Added sender", ip_answer, port_answer, "to the view")
        await self.pull_view(ip_answer, port_answer)

    async def handle_pullview(self, msg):
        """
        Handle a PULLVIEW message. Add content to the view.

        :param msg: PULLVIEW message including header
        """
        self.pull_done.set()  # set event for the maintain view thread
        size = int.from_bytes(msg[:2], "big")
        if size != len(msg):
            return  # handle bad message
        data = msg[12:size]
        address = b""
        i = 0
        while i < len(data):
            byte = int.to_bytes(data[i], 1, "big")
            if byte != b":":
                address += byte
            else:
                ip = address.decode()
                port = int.from_bytes(data[i + 1:i + 3], "big")
                is_myself = self.address == ip and self.port == port
                if not (await self.view.is_in_view((ip, port))) and not is_myself:
                    await self.view.force_update((ip, port))
                    self.debug_print('PULLVIEW: Added', ip, 'on port', port, 'to the view')
                i = i + 3
                address = b""
            i = i + 1

    async def pull_request(self, ip, port):
        """
        Sends a PULLREQUEST to a peer. Does not wait for an answer.

        :param ip: address of the receiver
        :param port: port of the receiver
        """
        try:
            r, w = await asyncio.open_connection(ip, port)
        except Exception as e:
            print(
                "Failed to build a connection with node {0}:{1} during pullrequest creating due to an error: {2}".format(
                    ip, port, e))
            await self.view.delete((ip, port))
            return
        challenge_msg = b""
        while challenge_msg == b"":
            challenge_msg = await r.read(8)
        challenge = challenge_msg[4:8]
        pull_request = int.to_bytes(GOSSIP_PULLREQUEST, 2, "big")
        ip_answer = bytes(list(map(int, socket.gethostbyname(self.address).split("."))))
        port_answer = int.to_bytes(self.port, 4, "big")
        size = int.to_bytes(20, 2, "big")
        msg = size + pull_request + ip_answer + port_answer + challenge
        msg = await self.find_nonsence(msg, b"")
        self.debug_print("Sending PULLREQUEST to", ip, "on", port)
        w.write(msg)

    async def pull_view(self, ip, port):
        """
        Sends a PULLVIEW to a peer.

        :param ip: address of the receiver
        :param port: port of the receiver
        """
        peers = await self.view.get_view()
        view = b''
        for peer in peers:
            view += peer[0].encode() + b":" + int.to_bytes(peer[1], 2, "big") + b";"
        try:
            r, w = await asyncio.open_connection(ip, port)
        except Exception as e:
            print("Failed to build a connection with node {0}:{1} during pullView creating due to an error: {2}".format(
                ip, port, e))
            await self.view.delete((ip, port))
            return
        challenge_msg = b""
        while challenge_msg == b"":
            challenge_msg = await r.read(8)
        challenge = challenge_msg[4:8]
        size = int.to_bytes(12 + len(view), 2, "big")
        push_view = int.to_bytes(GOSSIP_PULLVIEW, 2, "big")
        msg = size + push_view + challenge
        msg = await self.find_nonsence(msg, view)
        self.debug_print("Sending PULLVIEW to", ip, "on", port)
        w.write(msg)

    async def handle_forward_message(self, msg_body):
        """
        Handle an incoming FORWARD message.

        Passes message on to the apiHandler and waits for validation. Then passes it on to the peers.

        :param msg_body: FORWARD message without header
        :type msg_body: bytes
        """
        dtype = msg_body[6:8]
        ttl = msg_body[4]
        data = msg_body[16:]
        msg_id = await self.apihandler.send_notifications(dtype, data)
        valid = await self.apihandler.await_validation(msg_id)
        if valid:
            await self.forward_message(ttl, dtype, data)

    async def forward_message(self, ttl, datatype, data):
        """
        Forward a message to the peers. Reduces time to live.

        :param ttl: current time to live
        :type ttl: int
        :param datatype: datatype of the message
        :type datatype: bytes
        :param data: data of the message
        :type data: bytes
        """
        ttl = ttl - 1
        if ttl == 0:
            return
        ttl = int.to_bytes(ttl, 1, "big")
        reserved = int.to_bytes(0, 1, "big")
        msg_body = ttl + reserved + datatype
        size = int.to_bytes(4 + len(msg_body) + 8 + len(data), 2, "big")
        message_type = int.to_bytes(GOSSIP_FORWARD, 2, "big")
        header = size + message_type
        msg = header + msg_body
        view = await self.view.get_view()
        for (ip, port) in view:
            try:
                r, w = await asyncio.open_connection(ip, port)
            except ConnectionRefusedError:  # peer died
                await self.view.delete((ip, port))
                continue
            challenge_msg = b""
            while challenge_msg == b"":
                challenge_msg = await r.read(8)
            challenge = challenge_msg[4:8]
            msg = await self.find_nonsence(msg + challenge, data)
            w.write(msg)
            w.close()
            await w.wait_closed()

    async def find_nonsence(self, msg, view):
        """
        Add a nonce to the message such that the hash starts with "000000".

        :param msg: part of the message before the nonce
        :type msg: bytes
        :param view: part of the message after the nonce
        :type view: bytes
        :return: complete message with correct hash
        :rtype: bytes
        """
        nonce = [0, 0, 0, 0]
        msgTry = msg + bytes(nonce) + view
        while hashlib.sha256(msgTry).hexdigest()[0:2] != "00":
            if nonce[3] == 255:
                nonce[2] += 1
                nonce[3] = 0
            elif nonce[2] == 255:
                nonce[1] += 1
                nonce[2] = 0
            elif nonce[1] == 255:
                nonce[0] += 1
                nonce[1] = 0
            else:
                nonce[3] += 1
            msgTry = msg + bytes(nonce) + view
        return msgTry
