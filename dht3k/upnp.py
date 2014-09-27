""" Trying to map the port """

from .log import l

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
        l.exception("Portmapping failed")
        return False
