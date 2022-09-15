import asyncio
import socket

GOSSIP_ANNOUNCE = 500
GOSSIP_NOTIFY = 501
GOSSIP_NOTIFICATION = 502
GOSSIP_VALIDATION = 503


class APIHandler:
    """
    Class for handling client messages. Manages subscribers.

    Attributes:
        subscribers_lock (asyncio.Lock): lock for the subscribers dict
        subscribers (dict): contains a list of the subscribers for every dtype
        pending_validation_lock (asyncio.Lock): lock for the pending validation dict
        pending_validation (dict): contains the validation state for invalid or pending notifications
        server_address (string): ip under which the api-server is reachable
        server_port (int): port under which the api-server is reachable
        forward_method (function): function of the GossipPeer that forwards the message to peers
        verbose (bool): print debug messages
        server (asyncio.coroutine): server that listens for incoming client messages
        message_counter (int): counter to give each message a unique id
        counter_lock (asyncio.Lock): lock for the message counter
    """

    def __init__(self, forward_method, server_address, server_port, verbose=False):
        self.subscribers_lock = asyncio.Lock()
        self.subscribers = {}
        self.pending_validation_lock = asyncio.Lock()
        self.pending_validation = {}
        self.counter_lock = asyncio.Lock()
        self.message_counter = 0
        self.server_address = server_address
        self.server_port = server_port
        self.forward_method = forward_method
        self.verbose = verbose
        self.server = asyncio.start_server(self.event_loop_routine, host=self.server_address, port=self.server_port,
                                           family=socket.AddressFamily.AF_INET)

    def debug_print(self, *s):
        """
        Print debug messages with address of the peer.

        :param s: message to print
        """
        if self.verbose:
            sender = "[APIHandler " + str(self.server_address) + ':' + str(self.server_port) + ']:'
            print(sender, *s)

    async def remove_subscriber(self, sub):
        """
        Remove a client from subscriptions to all datatypes.

        :param sub: (reader, writer) of the subscriber to remove
        """
        async with self.subscribers_lock:
            for dtype in self.subscribers.keys():
                if sub in self.subscribers[dtype]:
                    self.subscribers[dtype].remove(sub)

    async def add_subscriber(self, sub, dtype):
        """
        Add a client to the subscription list of a datatype.

        :param sub: (reader, writer) of the subscriber
        :param dtype: datatype to add the client to
        """
        async with self.subscribers_lock:
            if dtype in self.subscribers.keys():
                subs = self.subscribers[dtype]
                if sub not in subs:
                    subs.append(sub)
                else:
                    pass  # handle double subscription
            else:
                self.subscribers[dtype] = [sub]

    async def get_subscribers(self, dtype):
        """Returns all subscribers of the given datatype"""
        async with self.subscribers_lock:
            if dtype not in self.subscribers.keys():
                res = []
            else:
                res = self.subscribers[dtype].copy()
        return res

    async def check_validation_state(self, msg_id):
        """
        Check the validation state for a specific message.

        Returns 'invalid' if at least one client sent an invalid validation. Returns 'pending' if not all clients
        have answered yet. Returns 'valid' if all clients answered positively.

        :param msg_id: id of the message to be checked
        :return: 'invalid', 'valid' or 'pending'
        """
        async with self.pending_validation_lock:
            for (r, w, mid) in self.pending_validation.keys():
                if mid == msg_id and self.pending_validation[(r, w, mid)] == 'invalid':
                    return 'invalid'

            for (r, w, mid) in self.pending_validation.keys():
                if mid == msg_id and self.pending_validation[(r, w, mid)] == 'pending':
                    return 'pending'
        return 'valid'

    async def await_validation(self, msg_id):
        """
        Wait for all clients to send a validation for a given message.

        :param msg_id: message id of the message that we wait for
        :return: True if all clients answered positively, False if invalid or no response from at least one client.
        """
        i = 100
        while await self.check_validation_state(msg_id) == 'pending':
            i = i - 1
            if i <= 0:
                self.debug_print("Timeout, received no validation for message", msg_id)
                return False  # handle no response
            await asyncio.sleep(1)
        if await self.check_validation_state(msg_id) == 'invalid':
            return False
        return True

    async def start(self):
        """Start the server."""
        event_loop = asyncio.get_event_loop()
        event_loop.create_task(self.server)

    async def shutdown(self):
        """Close the server."""
        self.server.close()

    async def close_client_connection(self, r, w, reason=''):
        """
        Close connection to a client after an error occured and print error message.

        :param r: reader of the client
        :type r: asyncio.StreamReader
        :param w: writer of the client
        :type w: asyncio.StreamWriter
        :param reason: message to print why the connection is closed
        :type reason: str
        """
        try:
            sender_ip, sender_port = w.get_extra_info('socket').getpeername()
        except OSError:
            sender_ip, sender_port = "<unkown>", "<unknown>"
        w.close()
        await w.wait_closed()
        await self.cleanup(r, w)
        message = ("Closing connection to", sender_ip, "on port", sender_port, ".")
        if reason == '':
            self.debug_print(*message)
        else:
            self.debug_print(reason, *message)

    async def cleanup(self, r, w):
        """
        Remove a client that closed the connection from all subscriber lists.

        :param r: reader of the client
        :type r: asyncio.StreamReader
        :param w: writer of the client
        :type w: asyncio.StreamWriter
        """
        await self.remove_subscriber((r, w))
        async with self.pending_validation_lock:
            copy = self.pending_validation.copy()
            for (reader, writer, msg_id) in copy:
                if reader == r and writer == w:
                    del self.pending_validation[(reader, writer, msg_id)]

    async def event_loop_routine(self, r, w):
        """
        Handle a client that connects to the server.

        Keeps the connection running until the client closes it or an error occurs. Calls the handler methods for
        all api-message types.

        :param r: reader of the client
        :param w: writer of the client
        """
        raddr, rport = w.get_extra_info('socket').getpeername()
        #print(f"[+] {raddr}:{rport} >>> Incoming connection")
        while True:
            header = await r.read(4)
            if header == b'':  # connection closed
                await self.cleanup(r, w)
                #print(f"[i] {raddr}:{rport} xxx Connection closed")
                return
            size = int.from_bytes(header[:2], "big")
            msg = header + await r.read(size-4)
            if not size == len(msg):
                await self.close_client_connection(r, w, "Received message of invalid length")
                return
            message_type = int.from_bytes(header[2:], "big")
            if message_type == GOSSIP_ANNOUNCE:
                await self.handle_announce(msg, r, w)
            elif message_type == GOSSIP_NOTIFY:
                await self.handle_notify(msg, r, w)
            elif message_type == GOSSIP_VALIDATION:
                await self.handle_validation(msg, r, w)
            else:
                await self.close_client_connection(r, w, "Received an invalid message type.")
                return

    async def handle_announce(self, msg, r, w):
        """
        Handle an incoming GOSSIP_ANNOUNCE.

        Check if the sender is subscribed and pass on its message to the GossipPeer.
        :param msg: GOSSIP_ANNOUNCE message including header
        :param r: reader of the sender
        :param w: writer of the sender
        """
        raddr, rport = w.get_extra_info('socket').getpeername()
        sender = (r, w)
        message_length = int.from_bytes(msg[:2], "big")
        if message_length != len(msg) or message_length < 8:
            await self.close_client_connection(r, w, "Received a gossip announce of invalid length.")
            return
        body = msg[4:]
        ttl = body[0]
        datatype = body[2:4]
        data = body[4:]
        subscribers = await self.get_subscribers(datatype)
        if sender not in subscribers:
            await self.close_client_connection(r, w, "Sender of gossip announce is not subscribed to datatype " + str(
                datatype) + ".")
            return
        loop = asyncio.get_event_loop()
        # send notifications to clients of the same peer
        loop.create_task(self.send_notifications(datatype, data, raddr, rport))
        # forward to other peers
        loop.create_task(self.forward_method(ttl, datatype, data))
        #print(f"[+] {raddr}:{rport} >>> GOSSIP_ANNOUNCE: ({datatype}:{data})")

    async def handle_notify(self, msg, r, w):
        """
        Handle an incoming GOSSIP_NOTIFY message. Add the sender to the subscribers list.

        :param msg: GOSSIP_NOTIFY message including header
        :param r: reader of the sender
        :param w: writer of the sender
        """
        raddr, rport = w.get_extra_info('socket').getpeername()
        sender = (r, w)
        if len(msg) != 8:
            await self.close_client_connection(r, w, "Received gossip notify of invalid length.")
            return
        datatype = msg[6:]
        await self.add_subscriber(sender, datatype)
        #print(f"[+] {raddr}:{rport} >>> GOSSIP_NOTIFY registered: ({datatype})")

    async def handle_validation(self, msg, r, w):
        """
        Handle GOSSIP_VALIDATION message.

        If it is positive, remove the client and message from the pending validation dict. If it is negative, set the
        entry in the dict to 'invalid'.

        :param msg: GOSSIP_VALIDATION message including header.
        :param r: reader of the sender
        :param w: writer of the sender
        """
        raddr, rport = w.get_extra_info('socket').getpeername()
        self.debug_print("Received validation")
        msg_id = int.from_bytes(msg[4:6], "big")
        flag = msg[7] & 1  # get last bit
        async with self.pending_validation_lock:
            if (r, w, msg_id) not in self.pending_validation:
                await self.close_client_connection(r, w, "Received validation with unknown ID.")
                return
            elif flag == 0:
                self.debug_print("Invalid notification!")
                self.pending_validation[(r, w, msg_id)] = 'invalid'
            else:
                del self.pending_validation[(r, w, msg_id)]
        valid = True if flag > 0 else False
        #print(f"[+] {raddr}:{rport} >>> GOSSIP_VALIDATION: ({msg_id}:{valid})")

    async def send_notifications(self, dtype, data, a_addr=None, a_port=None):
        """
        Send out notifications to all subscribers.

        :param a_port: port of sender client to exclude, defaults to None
        :param a_addr: address of sender client to exclude, defaults to None
        :param dtype: datatype of the notification data
        :param data: data to notify the clients of
        :return: id of the notification
        """
        async with self.counter_lock:
            msg_id = self.message_counter
            self.message_counter += 1
        self.debug_print("Sending notifications")
        subscribers = await self.get_subscribers(dtype)
        msg_id_bytes = int.to_bytes(msg_id, 2, "big")
        mtype = int.to_bytes(GOSSIP_NOTIFICATION, 2, "big")
        size = int.to_bytes(8 + len(data), 2, "big")
        header = size + mtype
        body = msg_id_bytes + dtype + data
        for (r, w) in subscribers:
            raddr, rport = w.get_extra_info('socket').getpeername()
            if raddr == a_addr and rport == a_port:
                continue
            w.write(header + body)
            #print(f"[+] {raddr}:{rport} <<< GOSSIP_NOTIFICATION("
            #      + f"{msg_id}, {dtype}, {data})")
            async with self.pending_validation_lock:
                self.pending_validation[(r, w, msg_id)] = 'pending'
        return msg_id
