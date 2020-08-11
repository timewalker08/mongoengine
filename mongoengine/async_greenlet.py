
import functools
import socket

import greenlet
import pymongo.pool
from tornado import ioloop, iostream


def green_sock_method(method):
    @functools.wraps(method)
    def _green_sock_method(self, *args, **kwargs):
        self.child_gr = greenlet.getcurrent()
        main = self.child_gr.parent
        assert main, "Should be on child greenlet"

        def closed(gr):
            if not gr.dead:
                gr.throw(socket.error("Close called, killing mongo operation"))

        self.stream.set_close_callback(functools.partial(closed, self.child_gr))

        try:
            method(self, *args, **kwargs)
            socket_result = main.switch()
            return socket_result
        finally:
            self.stream.set_close_callback(None)

    return _green_sock_method



class GreenletSocket(object):

    def __init__(self, sock, io_loop):
        self.io_loop = io_loop
        self.stream = iostream.IOStream(sock, io_loop=io_loop)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        self.timeout = timeout

    @green_sock_method
    def connect(self, pair):
        # do the connect on the underlying socket asynchronously...
        self.stream.connect(pair, greenlet.getcurrent().switch)

    def sendall(self, data):
        # do the send on the underlying socket synchronously...
        try:
            self.stream.write(data)
        except IOError as e:
            raise socket.error(str(e))

        if self.stream.closed():
            raise socket.error("connection closed")

    def recv(self, num_bytes):
        return self.recv_async(num_bytes)

    @green_sock_method
    def recv_async(self, num_bytes):
        # do the recv on the underlying socket... come back to the current
        # greenlet when it's done
        return self.stream.read_bytes(num_bytes, greenlet.getcurrent().switch)

    def close(self):
        # since we're explicitly handling closing here, don't raise an exception
        # via the callback
        self.stream.set_close_callback(None)

        sock = self.stream.socket
        try:
            try:
                self.stream.close()
            except KeyError:
                # Tornado's _impl (epoll, kqueue, ...) has already removed this
                # file descriptor from its dict.
                pass
        finally:
            # Sometimes necessary to avoid ResourceWarnings in Python 3:
            # specifically, if the fd is closed from the OS's view, then
            # stream.close() throws an exception, but the socket still has an
            # fd and so will print a ResourceWarning. In that case, calling
            # sock.close() directly clears the fd and does not raise an error.
            if sock:
                sock.close()

    def fileno(self):
        return self.stream.socket.fileno()


class GreenletPool(pymongo.pool.Pool):
    """A simple connection pool of GreenletSockets.
    """
    def __init__(self, *args, **kwargs):
        io_loop = kwargs.pop('io_loop', None)
        self.io_loop = io_loop if io_loop else ioloop.IOLoop.instance()
        pymongo.pool.Pool.__init__(self, *args, **kwargs)

        # HACK [adam Dec/6/14]: need to use our IOLoop/greenlet semaphore
        #      implementation, so override what Pool.__init__ sets
        #      self._socket_semaphore to here
        #self._socket_semaphore = GreenletBoundedSemaphore(self.max_size)

    def create_connection(self):
        """Copy of BasePool.connect()
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"

        host, port = self.address

        # Don't try IPv6 if we don't support it. Also skip it if host
        # is 'localhost' (::1 is fine). Avoids slow connect issues
        # like PYTHON-356.
        family = socket.AF_INET
        if socket.has_ipv6 and host != 'localhost':
            family = socket.AF_UNSPEC

        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res
            green_sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                green_sock = GreenletSocket(sock, self.io_loop)

                # GreenletSocket will pause the current greenlet and resume it
                # when connection has completed
                #green_sock.settimeout(self.conn_timeout)
                green_sock.connect(sa)
                #green_sock.settimeout(self.net_timeout)
                return green_sock
            except socket.error as e:
                err = e
                if green_sock is not None:
                    green_sock.close()

        if err is not None:
            # pylint: disable=E0702
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6.
            raise socket.error('getaddrinfo failed')


class GreenletClient(object):
    client = None

    @classmethod
    def sync_connect(cls, *args, **kwargs):
        """
            Makes a synchronous connection to pymongo using Greenlets

            Fire up the IOLoop to do the connect, then stop it.
        """

        assert not greenlet.getcurrent().parent, "must be run on root greenlet"

        def _inner_connect(io_loop, *args, **kwargs):
            # asynchronously create a MongoClient using our IOLoop
            try:
                kwargs['use_greenlets'] = False
                kwargs['_pool_class'] = GreenletPool
                #kwargs['_event_class'] = functools.partial(GreenletEvent, io_loop)
                cls.client = pymongo.mongo_client.MongoClient(*args, **kwargs)
            except:
                print("Failed to connect to MongoDB")
            finally:
                io_loop.stop()

        # clear cls.client so we can't return an old one
        if cls.client is not None:
            try:
                # manually close old unused connection
                cls.client.close()
            except:
                print("Clearing old pymongo connection")

        cls.client = None

        # do the connection
        io_loop = ioloop.IOLoop.instance()
        conn_gr = greenlet.greenlet(_inner_connect)

        # run the connect when the ioloop starts
        io_loop.add_callback(functools.partial(conn_gr.switch,
                                               io_loop, *args, **kwargs))

        # start the ioloop
        io_loop.start()

        return cls.client
