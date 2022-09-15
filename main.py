import os
from GossipPeer import GossipPeer
import argparse
import asyncio
import configparser

def main():
    path = "./configurations.ini"

    # parse commandline arguments
    usage_string = ("Run a GOSSIP module mockup with local info exchange.\n\n"
                    + "Multiple API clients can connect to this same instance.")
    cmd = argparse.ArgumentParser(description=usage_string)
    
    cmd.add_argument("-c", "--path", help="Path to the configurations file")
    args = cmd.parse_args()

    if args.path is not None:
        path = args.path

    file = open(path, "r")
    lines = file.readlines()

    #remove not needed lines
    while not lines[0].startswith("["):
        lines = lines[1:]

    temp = open("./temp.ini", "w")
    temp.writelines(lines)

    file.close()
    temp.close()
    
    config = configparser.ConfigParser()		
    config.read("./temp.ini")

    os.remove("./temp.ini")
    
    cache_size = config["gossip"]["cache_size"]

    degree = config["gossip"]["degree"]

    bootstrapper = config["gossip"]["bootstrapper"]
    bootstrapper_ip = bootstrapper.split(":")[0]
    if bootstrapper_ip.startswith("["):
        bootstrapper_ip = bootstrapper_ip[1:-1]
    bootstrapper_port = bootstrapper.split(":")[1]

    p2p_address = config["gossip"]["p2p_address"]
    p2p_ip = p2p_address.split(":")[0]
    if p2p_ip.startswith("["):
        p2p_ip = p2p_ip[1:-1]
    p2p_port = p2p_address.split(":")[1]

    api_address = config["gossip"]["api_address"]
    api_ip = api_address.split(":")[0]
    if api_ip.startswith("["):
        api_ip = api_ip[1:-1]
    api_port = api_address.split(":")[1]


    p = GossipPeer(p2p_ip, int(p2p_port), bootstrapper_ip, int(bootstrapper_port), api_ip, int(api_port), verbose=False, degree=int(degree), cache_size=int(cache_size))
    event_loop = asyncio.get_event_loop()
    event_loop.create_task(p.run())
    try:
        event_loop.run_forever()
    except KeyboardInterrupt as e:
        print("[i] Received SIGINT, shutting down...")
        event_loop.stop()


if __name__ == '__main__':
    main()
