=================
Kademlia-like DHT
=================

.. image:: https://badge.fury.io/py/dht3k.png
    :target: http://badge.fury.io/py/dht3k

.. image:: https://travis-ci.org/ganwell/dht3k.png?branch=master
    :target: https://travis-ci.org/ganwell/dht3k

.. image:: https://pypip.in/d/dht3k/badge.png
    :target: https://pypi.python.org/pypi/dht3k

.. image:: http://b.repl.ca/v1/coverage-100%25_required-brightgreen.png

DHT with Python 2/3 support and no heavy dependencies based on Isaac Zafuta's
pydht: https://github.com/isaaczafuta/pydht

Basic Usage
-----------

To use DHT3k in a project::

    import dht3k
    dht = dht3k.DHT(zero_config=True)
    dht["key"] = b"value"

Optional dependencies
---------------------

To improve performance and quality of the service install these modules:

* miniupnpc
* kyotocabinet


Features
--------

* Python 2/3/pypy support
* Zero config mode
* Address discovery
* NAT/Firewall detection
* Well known bootstrap node
* Optional async interface
* Disk storage (memory storage optional)
* IPv6 and IPv6/4 convergence
* UPnP support
* NAT Optimization: NATed/firewalled peers are moved to the end of the routing
  table faster. This should improve performance for both firewalled and 
  well connected peers. Also well connected nodes are returned first
* Less latency and waiting than standard Kademlia
* msgpack for wire protocol
* ThreadPool based
