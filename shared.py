from enum import Enum
from collections.abc import ByteString
import struct
from PythonLib.MyBytesIO import BIO

class MsgType(Enum):
	# client requests
	VERSION = 1
	REQ_LIMIT = 2
	CHDIR = 3
	BULKOP_BEGIN = 4
	BULKOP_CLOSE = 8
	CHUNK = 9
	EXIT = 10
	# server responses
	VERSION_RESP = 100
	LIMIT_RESP = 101
	GEN_RESULT = 102
	BULKOP_RESULTS = 103
	BULKOP_CLOSE_RESULTS = 104

class OpType(Enum):
	WRITE = 1
	SYMLINK = 2
	DELETE = 10

_readall_buff = BIO(256)

class BuffManager:
	def __init__(self, buff):
		if not isinstance(buff, BIO):
			raise TypeError("Unsupported backing buffer object")
		self._data = buff
		self._lastmsglenpos = None

	def begin_msg(self, msgtype):
		if not isinstance(msgtype, MsgType):
			raise ValueError("Unknown msgtype")
		self.append_byte(msgtype.value)
		self._data.seek(4, 1)
		self._lastmsglenpos = self._data.tell()

	def end_msg(self):
		# update msg size
		curpos = self._data.tell()
		msgsize = curpos - self._lastmsglenpos
		self._data.seek(self._lastmsglenpos - 4)
		self.append_uint(msgsize)
		self._data.seek(curpos)

	def append_bytes(self, v):
		if not isinstance(v, ByteString):
			raise TypeError("Expected byte-like type")
		self._data.write(v)

	def append_byte(self, v):
		if isinstance(v, ByteString):
			if len(v) != 1:
				raise ValueError("Must be one byte")
			self._data.write(v)
		elif isinstance(v, int):
			self._data.write(bytes([v]))
		else:
			raise TypeError("Expected bytes or int")

	def append_uint(self, v):
		self._data.write(struct.pack("=I", v))

	def append_sint(self, v):
		self._data.write(struct.pack("=i", v))

	def append_huint(self, v):
		self._data.write(struct.pack("=H", v))

	def append_hsint(self, v):
		self._data.write(struct.pack("=h", v))

	def getbuffer(self):
		return self._data.getbuffer()[:self._data.tell()]

def readinto_all(fh, dst):
	if not isinstance(dst, memoryview):
		raise TypeError("Destination must be a memoryview")
	total = 0
	while len(dst):
		rd = fh.readinto(dst)
		if not rd: # premature EOF
			break
		total += rd
		dst = dst[rd:]
	return total

def read_all(fh, sz):
	if sz > _readall_buff.capacity():
		raise ValueError("Cannot read larger than internal buffer")
	_readall_buff.seek(0)
	_readall_buff.set_limit(sz)
	buff = _readall_buff.getbuffer()
	rd = readinto_all(fh, buff)
	return _readall_buff.read(rd)

def write_all(fh, data):
	if not isinstance(data, memoryview):
		data = memoryview(data)
	total = 0
	while len(data):
		wr = fh.write(data)
		if not wr:
			break
		total += wr
		data = data[wr:]
	return total
