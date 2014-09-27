""" Trying to map the port """


def try_map_port(port):
    """ Try to map a port """
    try:
        import miniupnpc
    except ImportError:
        return False
    try:
        upnp = miniupnpc.UPnP()
        upnp.discover()
        upnp.selectigd()

        b = upnp.addportmapping(
            port,
            'TCP',
            upnp.lanaddr,
            port,
            'dht3k port: %u' % port,
            '',
        )
        if b:
            return True
        return False
    except Exception:
        return False
