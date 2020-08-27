"""
Provides the codecs for parsing/sending messages.

See http://redis.io/topics/protocol for more info.
"""
import redis


class InputParser(redis.connection.BaseParser):
    """Subclasses the client PythonParser to spoof the internals of reading off a socket."""
    def __init__(self, lines, encoding=None):
        self.data = lines
        self.pos = 0
        self.encoding = encoding

    def read(self, _length=None):
        """Override; read from memory instead of a socket."""
        self.pos += 1
        data = self.data[self.pos - 1]
        if self.encoding:
            data.decode(self.encoding)
        return data

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            exception_class = self.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, redis.exceptions.ResponseError)
            return exception_class(response)
        return redis.exceptions.ResponseError(response)

    def read_response(self):
        raw = self.read()
        if not raw:
            raise ConnectionError("Socket closed on remote end")

        byte, response = raw[:1], raw[1:]

        if byte not in (b'-', b'+', b':', b'$', b'*'):
            raise redis.exceptions.InvalidResponse("Protocol Error: %r" % raw)

            # server returned an error
        if byte == b'-':
            response = response.decode('utf-8', errors='replace')
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
            # single value
        elif byte == b'+':
            pass
            # int value
        elif byte == b':':
            response = int(response)
            # bulk response
        elif byte == b'$':
            length = int(response)
            if length == -1:
                return None
            response = self.read()
            if len(response) != length:
                raise redis.exceptions.DataError("error data length")
            # multi-bulk response
        elif byte == b'*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in range(length)]
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        return response


class Response(object):
    """Writes data to callback as dictated by the Redis Protocol."""
    def __init__(self, write_callback):
        self.callback = write_callback
        self.dirty = False

    def encode(self, value):
        """Respond with data."""
        if isinstance(value, (list, tuple)):
            self._write('*%d\r\n' % len(value))
            for v in value:
                self._bulk(v)
        elif isinstance(value, (int, float)):
            self._write(':%d\r\n' % value)
        elif isinstance(value, bool):
            self._write(':%d\r\n' % (1 if value else 0))
        else:
            self._bulk(value)

    def status(self, msg="OK"):
        """Send a status."""
        self._write("+%s\r\n" % msg)

    def error(self, msg):
        """Send an error."""
        data = ['-', str(msg), "\r\n"]
        self._write("".join(data))

    def _bulk(self, value):
        """Send part of a multiline reply."""
        data = [b'$', str(len(value)).encode(), b'\r\n', value, b'\r\n']
        self._write(b"".join(data))

    def _write(self, data):
        if not self.dirty:
            self.dirty = True

        if type(data) is str:
            data = data.encode()

        self.callback(data)
