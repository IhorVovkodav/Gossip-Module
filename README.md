Gossip Module
================

The gossip module aims to disseminate local knowledge of a peer and maintain membership of the
peers in the network. 

## Installation

Install Python3 for your operating system.

Now acquire the latest PIP distribution for your operating system. 

## Structure

    .
    ├── GossipPeer.py
    ├── Tests.py
    ├── api.py  
    ├── main.py
    ├── README.md
    └── configurations.ini

The GossipPeer class is responsible for communication between multiple Gossip modules of various Peers. It sends and handles internal messages, wich are required for the comunnication with other Voidphone Application modules.

The Api class is responsible for communication between Gossip module and other Voidphone Application modules. It sends and handles extertnal messages sended from other modules in order to spread a local knowledge across the system.

Main file is an executable file wich takes a path to .INI file with gossip cinfigurations. If a file exists than it will read it and save the given configurations. Otherwise a standrad file "configurations.ini" will be used. Script in this file starts Gossip module, which begins to wait for external messages from any gossip clients.

## Usage

Please invoke the main file with the `-c` option to provide a path to your configurations file:

    python3 main.py -c path

or

    ./main.py -c path

The configuration file must consist of at least five attrbiutes for gossip module:
    1. cache_size: Maximum number of data items to be held as part of the peer’s knowledge base. Older items will be removed to ensure space for newer items if the peer’s
    knowledge base exceeds this limit.
    2. degree: Number of peers the current peer has to exchange information with.
    3. bootstrapper: the network address for bootstrapping-services
    4. p2p_address: the network address where the module listens for P2P connections (internal messages).
    5. api_address: the network address where the module listens for API connections (external messages).

Example: 

    [gossip]
    cache_size = 50
    degree = 30
    bootstrapper = p2psec.net.in.tum.de:6001
    p2p_address = 131.159.15.61:6001
    api_address = 131.159.15.61:7001
