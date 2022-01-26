import ftplib
import os
import socket
import sys
import time
from urllib.error import URLError
from urllib.parse import unquote, urlparse


def local_to_global_path(local_file_path, local_ftp_path):
    return 'ftp://' + socket.gethostname() + '/' + local_file_path.replace(local_ftp_path, '')


def ftp_fetch(url, local_ftp_path='/srv/ftp/', memory=True, delete=False):
    scheme, host, path, _, _, _ = urlparse(url)
    if scheme != 'ftp':
        raise OSError('ftp error: wrong URL, expect ftp://')
    if not host:
        raise OSError('ftp error: no host given')
    if not path:
        raise OSError('ftp error: no file path given')

    dirs = path.split('/')
    dirs = list(map(unquote, dirs))
    dirs, file = dirs[:-1], dirs[-1]
    if dirs and not dirs[0]:
        dirs = dirs[1:]
    dir = '/'.join(dirs)
    os.chdir(local_ftp_path + dir)
    handler = CacheFTPHandler()
    if memory:
        return handler.ftp_open(host, dir, file, memory, delete)

    handler.ftp_open(host, dir, file, memory, delete)
    return local_ftp_path + dir + '/' + file


def splituser(host):
    """splituser('user[:passwd]@host[:port]') --> 'user[:passwd]', 'host[:port]'."""
    user, delim, host = host.rpartition('@')
    return (user if delim else None), host


def splitpasswd(user):
    """splitpasswd('user:passwd') -> 'user', 'passwd'."""
    user, delim, passwd = user.partition(':')
    return user, (passwd if delim else None)


# Borrowed from urllib.request
class FTPHandler:
    def ftp_open(self, host, dir, file, memory=True, delete=False):
        # username/password handling
        user, host = splituser(host)
        if user:
            user, passwd = splitpasswd(user)
        else:
            passwd = None
        host = unquote(host)
        user = user or ''
        passwd = passwd or ''

        try:
            host = socket.gethostbyname(host)
        except OSError as msg:
            raise URLError(msg)

        try:
            fw = self.connect_ftp(user, passwd, host, dir, socket._GLOBAL_DEFAULT_TIMEOUT)
            if memory:
                return fw.retrmemory(file, delete)
            fw.retrfile(file, delete)
        except ftplib.all_errors as exp:
            exc = URLError('ftp error: %r' % exp)
            raise exc.with_traceback(sys.exc_info()[2])

    def connect_ftp(self, user, passwd, host, dir, timeout):
        return ftpwrapper(user, passwd, host, dir, timeout)


class CacheFTPHandler(FTPHandler):
    # XXX would be nice to have pluggable cache strategies
    # XXX this stuff is definitely not thread safe
    def __init__(self):
        self.cache = {}
        self.timeout = {}
        self.soonest = 0
        self.delay = 60
        self.max_conns = 16

    def setTimeout(self, t):
        self.delay = t

    def setMaxConns(self, m):
        self.max_conns = m

    def connect_ftp(self, user, passwd, host, dirs, timeout):
        key = user, host, '/'.join(dirs), timeout
        if key in self.cache:
            self.timeout[key] = time.time() + self.delay
        else:
            self.cache[key] = ftpwrapper(user, passwd, host,
                                         dirs, timeout)
            self.timeout[key] = time.time() + self.delay
        self.check_cache()
        return self.cache[key]

    def check_cache(self):
        # first check for old ones
        t = time.time()
        if self.soonest <= t:
            for k, v in list(self.timeout.items()):
                if v < t:
                    self.cache[k].close()
                    del self.cache[k]
                    del self.timeout[k]
        self.soonest = min(list(self.timeout.values()))

        # then check the size
        if len(self.cache) == self.max_conns:
            for k, v in list(self.timeout.items()):
                if v == self.soonest:
                    del self.cache[k]
                    del self.timeout[k]
                    break
            self.soonest = min(list(self.timeout.values()))

    def clear_cache(self):
        for conn in self.cache.values():
            conn.close()
        self.cache.clear()
        self.timeout.clear()


class ftpwrapper:
    """Class used by open_ftp() for cache of open FTP connections."""

    def __init__(self, user, passwd, host, dir, timeout=None):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.dir = dir
        self.timeout = timeout
        try:
            self.ftp = ftplib.FTP(self.host, timeout=self.timeout)
            self.ftp.set_pasv(False)
            self.ftp.login(self.user, self.passwd)
            self.ftp.cwd(self.dir)
        except ftplib.error_perm as reason:
            raise URLError('ftp error: %r' % reason).with_traceback(
                sys.exc_info()[2])

    def retrmemory(self, file, delete=False):
        data = []
        def read_block(block):
            data.append(block)
        try:
            self.ftp.voidcmd('TYPE I')
            cmd = 'RETR ' + file
            self.ftp.retrbinary(cmd, read_block)
            if delete:
                self.ftp.delete(file)
            return b''.join(data)
        except ftplib.error_perm as reason:
            raise URLError('ftp error: %r' % reason).with_traceback(
                sys.exc_info()[2])

    def retrfile(self, file, delete=False):
        try:
            self.ftp.voidcmd('TYPE I')
            with open(file + '.tmp', 'wb') as fp:
                cmd = 'RETR ' + file
                self.ftp.retrbinary(cmd, fp.write)
            if delete:
                self.ftp.delete(file)
            os.rename(file + '.tmp', file)
        except ftplib.error_perm as reason:
            raise URLError('ftp error: %r' % reason).with_traceback(
                sys.exc_info()[2])

    def close(self):
        try:
            self.ftp.close()
        except ftplib.all_errors:
            pass
