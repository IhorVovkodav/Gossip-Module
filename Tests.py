import asyncio
from multiprocessing.connection import wait
from time import sleep

from GossipPeer import GossipPeer

GOSSIP_ANNOUNCE = 500
GOSSIP_NOTIFY = 501
GOSSIP_NOTIFICATION = 502
GOSSIP_VALIDATION = 503

n_peers = 5


def main():
    """Run all tests. Define and prepare peers and start tests"""
    # define peers
    peers = []
    min_port = 9001
    min_api_port = 7001
    event_loop = asyncio.get_event_loop()
    for i in range(n_peers):
        p = GossipPeer('localhost', min_port, api_port=min_api_port, auto_gossip=False, verbose=True)
        peers.append(p)
        min_port += 1
        min_api_port += 1
        event_loop.create_task(p.run())

    # define tests
    event_loop.create_task(all_tests(peers))
    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        for task in asyncio.all_tasks(event_loop):
            task.cancel()
        event_loop.close()
    print("Testing done")


async def all_tests(peers):
    """
    List all tests to be run here. It shuts the peers down and end the program afterwards.

    :param peers: peers to test.
    """
    await apitest(peers[0])
    await two_peers_test(peers[0], peers[1])
    await auto_gossip_test(peers, n_peers)
    for peer in peers:
        await peer.shutdown()
    asyncio.get_event_loop().stop()


async def apitest(p):
    """
    Tests the APIHandler.

    :param p: peer to test
    """
    print("Starting API Test")
    await p.view.reset_view()
    await apitest_not_subscribed(p)
    await apitest_bad_message_format(p)
    await apitest_invalid_message_type(p)
    await apitest_notify(p)
    await apitest_notification(p)
    await apitest_bad_validation(p)
    print("API test done")


async def apitest_not_subscribed(p):
    """
    Test unsubscribed announce.

    Test if the APIHandler closes the connection to a peer that sends an announce for a datatype it is not
    subscribed to.

    :param p: peer to be tested
    :raises AssertionError: if the client connection is not closed after 2 seconds.
    """
    print("Sending announce without notify")
    ip = p.apihandler.server_address
    port = p.apihandler.server_port

    #Our try to connect with a peer can happen before API listener begins to work
    while True:
        try:
            reader, writer = await asyncio.open_connection(ip, port)
            break
        except:
            continue

    send_announce(writer)  # Not subscribed
    await asyncio.sleep(2)
    assert await reader.read(1) == b''  # it sends an empty byte if the connection os closed


async def apitest_bad_message_format(p):
    """
    Test if the connection to a client that sends a bad message format is closed.

    :param p: peer to be tested.
    :raises AssertionError: if the client connection was not closed after 2 seconds.
    """
    print("Sending bad message")
    ip = p.apihandler.server_address
    port = p.apihandler.server_port
    reader, writer = await asyncio.open_connection(ip, port)
    message = "This is a bad message".encode()
    writer.write(message)
    await asyncio.sleep(2)
    assert await reader.read(1) == b''


async def apitest_invalid_message_type(p):
    """
    Test connection to a client that sends a message with invalid message type is closed.

    :param p: peer to be tested
    :raises AssertionError: if the connection is not closed within 2 seconds
    """
    print("Sending message with invalid message type")
    ip = p.apihandler.server_address
    port = p.apihandler.server_port
    reader, writer = await asyncio.open_connection(ip, port)
    mtype = int.to_bytes(1000, 2, "big")
    data = "MESSAGE!".encode()
    data_type = int.to_bytes(2, 2, "big")
    reserved = int.to_bytes(0, 1, "big")
    ttl = int.to_bytes(10, 1, "big")
    size = int.to_bytes((8 + len(data)), 2, "big")
    msg = size + mtype + ttl + reserved + data_type + data
    writer.write(msg)
    send_announce(writer)  # server should end connection
    await asyncio.sleep(2)
    assert await reader.read(1) == b''


async def apitest_notify(p):
    """
    Test if notifications work correctly.

    Test if subscribers are empty before and have length 1 after sending a notify.
    :param p: peer to be tested
    :raises AssertionError: if the test condition is not met
    """
    print("Sending notify")
    reader, writer = await asyncio.open_connection('localhost', 7001)
    dtype = int.to_bytes(4, 2, "big")
    subs = await p.apihandler.get_subscribers(dtype)
    assert subs == []
    send_notify(writer, dtype=4)
    await asyncio.sleep(1)
    subs = await p.apihandler.get_subscribers(dtype)
    assert len(subs) == 1
    print("Notify successful")
    writer.close()
    await writer.wait_closed()


async def apitest_notification(p):
    """
    Test if notifications work correctly.

    Send a notify and make the peer send out notifications. Test if the received notification has the correct
    message type, id, datatype and message. Check if the validation state is pending. Then send a positive validation
    and check if await_validation returns true.

    :param p: peer to be tested
    :raises AssertionError: if one of the test conditions was not met.
    """
    event_loop = asyncio.get_event_loop()
    reader, writer = await asyncio.open_connection('localhost', 7001)
    send_notify(writer, 2)
    dtype = int.to_bytes(2, 2, "big")
    await asyncio.sleep(2)
    message = "This is a notification".encode()
    msg_id = await p.apihandler.send_notifications(dtype, message)
    notification_header = await reader.read(4)
    print("Receiving notification")
    assert notification_header[2:4] == int.to_bytes(GOSSIP_NOTIFICATION, 2, "big")
    notification = await reader.read(int.from_bytes(notification_header[:2], "big"))
    assert int.from_bytes(notification[:2], "big") == msg_id
    assert notification[2:4] == dtype
    assert notification[4:] == message
    print("Answering with validation")
    assert await p.apihandler.check_validation_state(msg_id) == 'pending'
    send_validation(writer, msg_id, valid=True)
    assert await p.apihandler.await_validation(msg_id)
    print("Notification is validated")
    writer.close()
    await writer.wait_closed()


async def apitest_bad_validation(p):
    """
    Test if the APIHandler reacts to a negative validation.

    Make the peer send notifications. Check if the validation state is 'pending'. Then send out a negative validation.
    Check if the validation state is 'invalid'.

    :param p: peer to be tested
    :raises AssertionError: if one of the test conditions was not met
    """
    event_loop = asyncio.get_event_loop()
    reader, writer = await asyncio.open_connection('localhost', 7001)
    send_notify(writer, 2)
    dtype = int.to_bytes(2, 2, "big")
    await asyncio.sleep(2)
    message = "This is a notification".encode()
    msg_id = await p.apihandler.send_notifications(dtype, message)
    notification_header = await reader.read(4)
    await reader.read(int.from_bytes(notification_header[:2], "big"))
    print("Sending negative validation")
    assert await p.apihandler.check_validation_state(msg_id) == 'pending'
    send_validation(writer, msg_id, valid=False)
    await asyncio.sleep(2)
    assert await p.apihandler.check_validation_state(msg_id) == 'invalid'
    print("Invalid message is stopped")


async def two_peers_test(p1, p2):
    """
    Test the interaction between 2 peers. Auto gossip is off to test individual pushes/pulls.

    :param p1: one of the peers to be tested
    :param p2: the other peer to be tested
    """
    print("Starting test with 2 peers")
    #await two_peers_pull_test(p1, p2)
    #await two_peers_push_test(p1, p2)
    await two_peers_forward_test(p1, p2)
    print("Done testing with 2 peers")


async def two_peers_pull_test(p1, p2):
    """
    Test pull gossip. p1 sends a pull request to p2.

    :param p1: peer 1
    :param p2: peer 2
    :raises AssertionError: if the view is not as expected after a pull
    """
    print("Testing pull gossip")
    await p1.view.reset_view()
    await p2.view.reset_view()
    await p1.view.try_update(("127.0.1.1", 11111))  # p1 already knows this one
    await p2.view.try_update(("127.0.1.1", 11111))
    await p2.view.try_update(("127.0.2.1", 22222))
    await p2.view.try_update(("127.0.3.1", 33333))
    await p2.view.try_update(("127.0.4.1", 44444))
    print(await p1.view.get_view())
    expected_view = [('127.0.1.1', 11111), ('127.0.2.1', 22222), ('127.0.3.1', 33333), ('127.0.4.1', 44444)]
    # p1 asks p2 to send its view
    await p1.pull_request(p2.address, p2.port)
    i = 0
    for i in range(30):
        await asyncio.sleep(2)
        if await p1.view.get_view():
            assert await p1.view.get_view() == expected_view
            print('Pull gossip test successful')
            break
    if i == 29:
        print('Timeout for pull gossip')


async def two_peers_push_test(p1, p2):
    """
    Test push gossip. p1 sends a pushview to p2

    :param p1: peer 1
    :param p2: peer 2
    :raises AssertionError: if one of the views is not as expected after a push
    """
    print("Testing push gossip")
    await p1.view.reset_view()
    await p2.view.reset_view()
    await p1.view.try_update(("127.0.1.1", 11111))
    await p1.view.try_update(("127.0.2.1", 22222))
    await p1.view.try_update(("localhost", 9002))
    await p2.view.try_update(("127.0.2.1", 22222))
    await p2.view.try_update(("127.0.3.1", 33333))
    await p2.view.try_update(("127.0.4.1", 44444))
    # p1 sends push view to 11111, 22222 and 9002
    # p1 deletes 11111 and 22222 due to inactivity
    # p2 has 22222, 33333 and 44444 and updates sender 9001 and 11111
    # p2's answer contains 22222, 33333 and 44444
    p1_expected_view = [("127.0.0.1", 9002), ("127.0.2.1", 22222),
                        ('127.0.3.1', 33333), ('127.0.4.1', 44444)]
    p2_expected_view = [('127.0.2.1', 22222), ('127.0.3.1', 33333), ('127.0.4.1', 44444), ('127.0.0.1', 9001),
                        ("127.0.1.1", 11111)]
    print("Trying to connect to 2 inactive peers:")
    await p1.push_view()
    i = 0
    p1_success, p2_success = False, False
    for i in range(30):
        await asyncio.sleep(2)
        if not p2_success and await p2.view.get_view():
            assert await p2.view.get_view() == p2_expected_view
            p2_success = True
        if not p1_success and await p1.view.get_view():
            assert await p1.view.get_view() == p1_expected_view
            p1_success = True
        if p2_success and p1_success:
            print('Push gossip test successful')
            break
    if i == 29:
        print('Timeout for push gossip test')


async def two_peers_forward_test(p1, p2):
    """
    Test the forwarding of a message. Send an announce to p1 and check notification of p2.

    :param p1: peer 1
    :param p2: peer 2
    :raises AssertionError: if the wrong message type or message was received or the validation is not awaited.
    """
    print("Testing forward message")
    await p1.view.reset_view()
    await p2.view.reset_view()
    await p1.view.try_update((p2.address, p2.port))  # p1 knows about p2
    # subscribe both peers to message type 2
    reader2, writer2 = await asyncio.open_connection('localhost', 7002)
    send_notify(writer2, dtype=2)
    reader1, writer1 = await asyncio.open_connection('localhost', 7001)
    send_notify(writer1, dtype=2)
    send_announce(writer1)  # p1 send message
    notification_header = await reader2.read(4)  # message is forwarded to p2 and client is notified
    notification = await reader2.read(int.from_bytes(notification_header[:2], "big"))
    msg_id = int.from_bytes(notification[:2], "big")
    assert notification[2:4] == int.to_bytes(2, 2, "big")  # check correct data type
    assert notification[4:] == "ANNOUNCE MESSAGE!".encode()  # check correct message
    send_validation(writer2, msg_id=msg_id, valid=True)
    assert await p2.apihandler.await_validation(msg_id)  # check if validation was received
    writer1.close()
    await writer1.wait_closed()
    writer2.close()
    await writer2.wait_closed()
    print("Forward test successful")


async def auto_gossip_test(peers, n):
    """
    Test n peers with automatic pushes and pulls.

    :param peers: peers to test
    :param n: number of peers at the start of the test
    """
    await line_test(peers, n)
    await random_network_test(peers, n)


async def line_test(peers, n):
    """
    Test a network of n peers with automatic pushes and pulls.

    In the beginning, peers are connected in a line (peer i knows about peer i+1). Test if the first peer learns about
    the last and the last learns about the first.

    :param peers: peers to be tested
    :param n: size of the network
    :raises AssertionError: if the last and first peer don't know each other after 60 seconds.
    """
    print("Starting auto gossip test")
    # reset view and start gossiping
    active_peers = peers[:n]
    for peer in active_peers:
        await peer.view.reset_view()
        peer.verbose = False
        peer.start_auto_gossip()
    # start with a line
    for i in range(len(active_peers)-1):
        await peers[i].view.try_update((peers[i+1].address, peers[i+1].port))
    k = 3  # seconds to wait between checks
    last_peer = active_peers[-1]
    first_peer = active_peers[0]
    found_last = False
    found_first = False
    for i in range(60//k):
        if not found_last and (last_peer.address, last_peer.port) in await first_peer.view.get_view():
            found_last = True
            print("Network after", i*k, "seconds")
            await print_network(active_peers)
            print("It took less than", i*k, "seconds for the first peer to know about the last")
        if not found_first and (first_peer.address, first_peer.port) in await last_peer.view.get_view():
            found_first = True
            print("Network after", i*k, "seconds")
            await print_network(active_peers)
            print("It took less than", i*k, "seconds for the last peer to know about the first")
        if found_last and found_first:
            break
        await asyncio.sleep(k)
    assert found_last
    assert found_first
    return


async def random_network_test(peers, n):
    pass


def testPushes(event_loop):
    event_loop.create_task(pushAnswerTest('127.0.0.1', 8000))
    event_loop.create_task(pushViewTest('127.0.0.1', 8001, 'localhost', 8000))


async def pushAnswerTest(myip, myport):
    peer = GossipPeer(myip, myport)
    await peer.view.try_update(("127.0.1.1", 11111))
    await peer.view.try_update(("127.0.2.1", 22222))
    await peer.view.try_update(("127.0.3.1", 33333))
    await peer.view.try_update(("127.0.4.1", 44444))


async def pushViewTest(myip, myport, peerip, peerport):
    peer = GossipPeer(myip, myport, api_port=7002)
    await peer.view.try_update(("127.0.2.1", 22222))
    await peer.view.try_update((peerip, peerport))
    while True:
        try:
            await peer.push_view()
            break
        except ConnectionRefusedError:
            print("fail")
            sleep(5)
        except Exception as e:
            raise e


def send_validation(w, msg_id, valid):
    """
    Helper function for a mock client to send a GOSSIP_VALIDATION to a peer.

    :param w: writer to send the validation to
    :param msg_id: id of the notification
    :param valid: True if the notification was valid, False if not
    """
    mtype = int.to_bytes(GOSSIP_VALIDATION, 2, "big")  # message type for testing is always 2
    msg_id = int.to_bytes(msg_id, 2, "big")  # message id for testing is always 2
    f = b'\x00\x01' if valid else b'\x00\x00'
    size = int.to_bytes(8, 2, "big")
    msg = size + mtype + msg_id + f
    w.write(msg)


def send_announce(w, ttl=10):
    """
    Helper function for a mock client to send a GOSSIP_ANNOUNCE to a peer.

    :param w: writer to send the message to
    :param ttl: time to live the announce should have, defaults to 10
    """
    data = "ANNOUNCE MESSAGE!".encode()
    data_type = int.to_bytes(2, 2, "big")  # datatype for tests is always 2
    reserved = int.to_bytes(0, 1, "big")
    ttl = int.to_bytes(ttl, 1, "big")
    gossip_announce = int.to_bytes(GOSSIP_ANNOUNCE, 2, "big")
    size = int.to_bytes((8 + len(data)), 2, "big")
    msg = size + gossip_announce + ttl + reserved + data_type + data
    w.write(msg)


def send_notify(w, dtype=2):
    """
    Helper function for a mock client to send a GOSSIP_NOTIFY to a peer.

    :param w: writer to send the message to
    :param dtype: datatype of the message, defaults to 2
    """
    data_type = int.to_bytes(dtype, 2, "big")
    reserved = int.to_bytes(0, 2, "big")
    mtype = int.to_bytes(GOSSIP_NOTIFY, 2, "big")
    size = int.to_bytes(8, 2, "big")
    w.write(size + mtype + reserved + data_type)


async def print_network(peers):
    """
    Print the views of all peers in the network on the console.

    :param peers: peers in the network
    """
    for i, peer in enumerate(peers):
        name = str(peer.address) + ":" + str(peer.port)
        view = [str(ip) + ":" + str(port) for ip, port in await peer.view.get_view()]
        print(name, "\t", view)

main()
