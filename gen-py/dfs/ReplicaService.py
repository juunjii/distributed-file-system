#
# Autogenerated by Thrift Compiler (0.19.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
    def get_file_size(self, fname):
        """
        Parameters:
         - fname

        """
        pass

    def get_file_chunk(self, fname, offset, chunk_size):
        """
        Parameters:
         - fname
         - offset
         - chunk_size

        """
        pass

    def writeFileChunk(self, fname, data, offset):
        """
        Parameters:
         - fname
         - data
         - offset

        """
        pass

    def request_file(self, fname, ip, port):
        """
        Parameters:
         - fname
         - ip
         - port

        """
        pass


class Client(Iface):
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

    def get_file_size(self, fname):
        """
        Parameters:
         - fname

        """
        self.send_get_file_size(fname)
        return self.recv_get_file_size()

    def send_get_file_size(self, fname):
        self._oprot.writeMessageBegin('get_file_size', TMessageType.CALL, self._seqid)
        args = get_file_size_args()
        args.fname = fname
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_get_file_size(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = get_file_size_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        raise TApplicationException(TApplicationException.MISSING_RESULT, "get_file_size failed: unknown result")

    def get_file_chunk(self, fname, offset, chunk_size):
        """
        Parameters:
         - fname
         - offset
         - chunk_size

        """
        self.send_get_file_chunk(fname, offset, chunk_size)
        return self.recv_get_file_chunk()

    def send_get_file_chunk(self, fname, offset, chunk_size):
        self._oprot.writeMessageBegin('get_file_chunk', TMessageType.CALL, self._seqid)
        args = get_file_chunk_args()
        args.fname = fname
        args.offset = offset
        args.chunk_size = chunk_size
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_get_file_chunk(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = get_file_chunk_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        raise TApplicationException(TApplicationException.MISSING_RESULT, "get_file_chunk failed: unknown result")

    def writeFileChunk(self, fname, data, offset):
        """
        Parameters:
         - fname
         - data
         - offset

        """
        self.send_writeFileChunk(fname, data, offset)
        self.recv_writeFileChunk()

    def send_writeFileChunk(self, fname, data, offset):
        self._oprot.writeMessageBegin('writeFileChunk', TMessageType.CALL, self._seqid)
        args = writeFileChunk_args()
        args.fname = fname
        args.data = data
        args.offset = offset
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_writeFileChunk(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = writeFileChunk_result()
        result.read(iprot)
        iprot.readMessageEnd()
        return

    def request_file(self, fname, ip, port):
        """
        Parameters:
         - fname
         - ip
         - port

        """
        self.send_request_file(fname, ip, port)
        self.recv_request_file()

    def send_request_file(self, fname, ip, port):
        self._oprot.writeMessageBegin('request_file', TMessageType.CALL, self._seqid)
        args = request_file_args()
        args.fname = fname
        args.ip = ip
        args.port = port
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_request_file(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = request_file_result()
        result.read(iprot)
        iprot.readMessageEnd()
        return


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["get_file_size"] = Processor.process_get_file_size
        self._processMap["get_file_chunk"] = Processor.process_get_file_chunk
        self._processMap["writeFileChunk"] = Processor.process_writeFileChunk
        self._processMap["request_file"] = Processor.process_request_file
        self._on_message_begin = None

    def on_message_begin(self, func):
        self._on_message_begin = func

    def process(self, iprot, oprot):
        (name, type, seqid) = iprot.readMessageBegin()
        if self._on_message_begin:
            self._on_message_begin(name, type, seqid)
        if name not in self._processMap:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return
        else:
            self._processMap[name](self, seqid, iprot, oprot)
        return True

    def process_get_file_size(self, seqid, iprot, oprot):
        args = get_file_size_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = get_file_size_result()
        try:
            result.success = self._handler.get_file_size(args.fname)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("get_file_size", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_get_file_chunk(self, seqid, iprot, oprot):
        args = get_file_chunk_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = get_file_chunk_result()
        try:
            result.success = self._handler.get_file_chunk(args.fname, args.offset, args.chunk_size)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("get_file_chunk", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_writeFileChunk(self, seqid, iprot, oprot):
        args = writeFileChunk_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = writeFileChunk_result()
        try:
            self._handler.writeFileChunk(args.fname, args.data, args.offset)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("writeFileChunk", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_request_file(self, seqid, iprot, oprot):
        args = request_file_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = request_file_result()
        try:
            self._handler.request_file(args.fname, args.ip, args.port)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("request_file", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

# HELPER FUNCTIONS AND STRUCTURES


class get_file_size_args(object):
    """
    Attributes:
     - fname

    """


    def __init__(self, fname=None,):
        self.fname = fname

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.fname = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('get_file_size_args')
        if self.fname is not None:
            oprot.writeFieldBegin('fname', TType.STRING, 1)
            oprot.writeString(self.fname.encode('utf-8') if sys.version_info[0] == 2 else self.fname)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(get_file_size_args)
get_file_size_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'fname', 'UTF8', None, ),  # 1
)


class get_file_size_result(object):
    """
    Attributes:
     - success

    """


    def __init__(self, success=None,):
        self.success = success

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.I64:
                    self.success = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('get_file_size_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.I64, 0)
            oprot.writeI64(self.success)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(get_file_size_result)
get_file_size_result.thrift_spec = (
    (0, TType.I64, 'success', None, None, ),  # 0
)


class get_file_chunk_args(object):
    """
    Attributes:
     - fname
     - offset
     - chunk_size

    """


    def __init__(self, fname=None, offset=None, chunk_size=None,):
        self.fname = fname
        self.offset = offset
        self.chunk_size = chunk_size

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.fname = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I64:
                    self.offset = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.chunk_size = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('get_file_chunk_args')
        if self.fname is not None:
            oprot.writeFieldBegin('fname', TType.STRING, 1)
            oprot.writeString(self.fname.encode('utf-8') if sys.version_info[0] == 2 else self.fname)
            oprot.writeFieldEnd()
        if self.offset is not None:
            oprot.writeFieldBegin('offset', TType.I64, 2)
            oprot.writeI64(self.offset)
            oprot.writeFieldEnd()
        if self.chunk_size is not None:
            oprot.writeFieldBegin('chunk_size', TType.I64, 3)
            oprot.writeI64(self.chunk_size)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(get_file_chunk_args)
get_file_chunk_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'fname', 'UTF8', None, ),  # 1
    (2, TType.I64, 'offset', None, None, ),  # 2
    (3, TType.I64, 'chunk_size', None, None, ),  # 3
)


class get_file_chunk_result(object):
    """
    Attributes:
     - success

    """


    def __init__(self, success=None,):
        self.success = success

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRING:
                    self.success = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('get_file_chunk_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRING, 0)
            oprot.writeBinary(self.success)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(get_file_chunk_result)
get_file_chunk_result.thrift_spec = (
    (0, TType.STRING, 'success', 'BINARY', None, ),  # 0
)


class writeFileChunk_args(object):
    """
    Attributes:
     - fname
     - data
     - offset

    """


    def __init__(self, fname=None, data=None, offset=None,):
        self.fname = fname
        self.data = data
        self.offset = offset

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.fname = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.data = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.offset = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('writeFileChunk_args')
        if self.fname is not None:
            oprot.writeFieldBegin('fname', TType.STRING, 1)
            oprot.writeString(self.fname.encode('utf-8') if sys.version_info[0] == 2 else self.fname)
            oprot.writeFieldEnd()
        if self.data is not None:
            oprot.writeFieldBegin('data', TType.STRING, 2)
            oprot.writeBinary(self.data)
            oprot.writeFieldEnd()
        if self.offset is not None:
            oprot.writeFieldBegin('offset', TType.I64, 3)
            oprot.writeI64(self.offset)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(writeFileChunk_args)
writeFileChunk_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'fname', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'data', 'BINARY', None, ),  # 2
    (3, TType.I64, 'offset', None, None, ),  # 3
)


class writeFileChunk_result(object):


    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('writeFileChunk_result')
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(writeFileChunk_result)
writeFileChunk_result.thrift_spec = (
)


class request_file_args(object):
    """
    Attributes:
     - fname
     - ip
     - port

    """


    def __init__(self, fname=None, ip=None, port=None,):
        self.fname = fname
        self.ip = ip
        self.port = port

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.fname = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.ip = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I32:
                    self.port = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('request_file_args')
        if self.fname is not None:
            oprot.writeFieldBegin('fname', TType.STRING, 1)
            oprot.writeString(self.fname.encode('utf-8') if sys.version_info[0] == 2 else self.fname)
            oprot.writeFieldEnd()
        if self.ip is not None:
            oprot.writeFieldBegin('ip', TType.STRING, 2)
            oprot.writeString(self.ip.encode('utf-8') if sys.version_info[0] == 2 else self.ip)
            oprot.writeFieldEnd()
        if self.port is not None:
            oprot.writeFieldBegin('port', TType.I32, 3)
            oprot.writeI32(self.port)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(request_file_args)
request_file_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'fname', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'ip', 'UTF8', None, ),  # 2
    (3, TType.I32, 'port', None, None, ),  # 3
)


class request_file_result(object):


    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('request_file_result')
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(request_file_result)
request_file_result.thrift_spec = (
)
fix_spec(all_structs)
del all_structs
