import sys
import os
import struct
import shared
from PythonLib.MyBytesIO import BIO

_fin = sys.stdin.buffer.raw
_fout = sys.stdout.buffer.raw

MAX_OFD = 200 # max open file handle
BUFF_SZ = 1_048_576

class _Server:
	def __init__(self):
		self._handler = {
			shared.MsgType.VERSION: self._handler_version,
			shared.MsgType.REQ_LIMIT: self._handler_req_limit,
			shared.MsgType.CHDIR: self._handler_chdir,
			shared.MsgType.BULKOP_BEGIN: self._handler_bulkop_begin,
			shared.MsgType.BULKOP_CLOSE: self._handler_bulkop_close,
			shared.MsgType.CHUNK: self._handler_chunk,
		}
		self._ophandler = {
			shared.OpType.WRITE: self._handler_openwrite,
			shared.OpType.SYMLINK: self._handler_create_symlink,
			shared.OpType.DELETE: self._handler_delete,
		}
		# pay attention to these size if adjusting above limits
		self._buff = BIO(BUFF_SZ)
		self._replybuff = BIO(BUFF_SZ // 2)
		self._replybuffman = shared.BuffManager(self._replybuff)
		self._bulkopactive = False
		self._bulkofd = {} # map fd: [file object, truncation required, write errno]

	def main(self):
		while True:
			cmd = shared.read_all(_fin, 1)
			if not cmd:
				# EOF (e.g. client terminated prematurely)
				break
			cmd = shared.MsgType(cmd[0])
			arglen = struct.unpack("=I", shared.read_all(_fin, 4))[0]
			if arglen > self._buff.capacity():
				raise ValueError(f"Input size of {arglen} bytes is too big")
			self._buff.set_limit(arglen)
			rd = shared.readinto_all(_fin, self._buff.getbuffer())
			if rd != arglen:
				raise ValueError(f"Got {rd} bytes expected {arglen} bytes")
			self._buff.seek(0)
			self._replybuff.seek(0)
			if cmd == shared.MsgType.EXIT:
				break
			self._handler[cmd]()

	def _handler_version(self):
		"""
		args: None
		returns:
			uint32_t version
		"""
		self._replybuffman.begin_msg(shared.MsgType.VERSION_RESP)
		self._replybuffman.append_uint(1) # version 1
		self._replybuffman.end_msg()
		_fout.write(self._replybuffman.getbuffer())

	def _handler_req_limit(self):
		"""
		args: None
		returns:
			uint32_t max outstanding write requests in bulkop
			uint32_t max arg length
		"""
		self._replybuffman.begin_msg(shared.MsgType.LIMIT_RESP)
		self._replybuffman.append_uint(MAX_OFD)
		self._replybuffman.append_uint(self._buff.capacity())
		self._replybuffman.end_msg()
		_fout.write(self._replybuffman.getbuffer())

	def _handler_chdir(self):
		"""
		args:
			string path
		returns:
			uint16_t errno
		"""
		fn = self._buff.read().decode('utf8')
		try:
			try:
				os.chdir(fn)
			except FileNotFoundError:
				# attempt to recreate the dir if it is not there
				os.makedirs(fn, exist_ok=True)
				os.chdir(fn)
			errno = 0
		except OSError as ex:
			errno = ex.errno
		self._replybuffman.begin_msg(shared.MsgType.GEN_RESULT)
		self._replybuffman.append_huint(errno)
		self._replybuffman.end_msg()
		_fout.write(self._replybuffman.getbuffer())

	def _handler_bulkop_begin(self):
		"""
		args: (list of openwrite/delete requests)
		returns: (list of openwrite/delete responses)
		"""
		if self._bulkopactive:
			raise RuntimeError("Previous bulk operations have not been finished")
		self._bulkopactive = True
		self._bulkofd.clear()

		self._replybuffman.begin_msg(shared.MsgType.BULKOP_RESULTS)

		while True:
			optype = self._buff.read(1)
			if not optype:
				break
			optype = shared.OpType(optype[0])
			# we still attempt to open the write even if the number of open handles
			# would exceed of what we've told client: OS will throw an EMFILE anyway
			# on such case.
			self._ophandler[optype]()

		self._replybuffman.end_msg()
		_fout.write(self._replybuffman.getbuffer())

	def _handler_bulkop_close(self):
		"""
		args: None
		returns:
			list of:
				int32_t fd
				uint16_t errno
		"""
		self._replybuffman.begin_msg(shared.MsgType.BULKOP_CLOSE_RESULTS)
		for fd, v in self._bulkofd.items():
			fh, _, errno = v
			fh.close()
			self._replybuffman.append_uint(fd)
			self._replybuffman.append_hsint(errno)
		self._replybuffman.end_msg()
		_fout.write(self._replybuffman.getbuffer())

	def _handler_chunk(self):
		"""
		args:
			uint32_t fd
			bytearray datalen
		returns: None
		"""
		if not self._bulkopactive:
			raise ValueError("Writing chunks when no open file")
		fd = struct.unpack("=i", self._buff.read(4))[0]
		# [file object, truncation needed, errno (from writing)]
		arr = self._bulkofd[fd]
		fh = arr[0]

		# truncate needed?
		if not arr[1]:
			arr[1] = True
			fh.truncate(0)
			fh.seek(0)

		# error check
		if arr[2]:
			# previous error occured: skip
			return

		try:
			fh.write(self._buff.read())
		except OSError as ex:
			arr[2] = ex.errno

	def _handler_delete(self):
		"""
		args:
			uint16_t fn_len
			string fn
		returns:
			uint16_t errno
		"""
		fn = self._read_string()
		errno = 0
		try:
			os.unlink(fn)
		except FileNotFoundError:
			# not a problem! the file is already gone anyway!
			pass
		except OSError as ex:
			errno = ex.errno
		self._replybuffman.append_huint(errno)

	def _handler_create_symlink(self):
		"""
		args:
			uint16_t fn_len
			string fn
			uint16_t target_len
			string target
		returns:
			uint16_t errno
		"""
		fn = self._read_string()
		target = self._read_string()
		try:
			_file_creation(fn, lambda: os.symlink(target, fn))
			errno = 0
		except OSError as ex:
			errno = ex.errno
		self._replybuffman.append_huint(errno)

	def _handler_openwrite(self):
		"""
		args:
			uint8_t executable (0 = regular, 1 = executable)
			uint16_t fn_len
			string fn
		returns:
			int32_t fd
			uint16_t errno
		"""
		executable = bool(self._buff.read(1)[0])
		fn = self._read_string()
		fh = [None]

		def _handler():
			fh[0] = open(fn, 'ab', buffering=0)
		try:
			_file_creation(fn, _handler)
			errno = 0
		except OSError as ex:
			errno = ex.errno

		fh = fh[0]
		self._replybuffman.append_sint(-1 if fh is None else fh.fileno())
		self._replybuffman.append_huint(errno)

		if fh:
			# OK
			# store (file object, truncated?, errno)
			self._bulkofd[fh.fileno()] = [fh, False, 0]
			try:
				_set_file_executable(fh, executable)
			except OSError as ex:
				print(f"Error setting mode for {fn}: {ex}", file=sys.stderr)

	def _read_string(self):
		"""
		Read 16 bit string length followed by the UTF8 string
		"""
		retlen = struct.unpack("=H", self._buff.read(2))[0]
		ret = self._buff.read(retlen).decode('utf8')
		return ret

def _set_file_executable(fh, executable):
	mode = os.fstat(fh.fileno()).st_mode & 0o777
	newmode = None
	if executable:
		if (mode & 0o111) != 0o111:
			newmode = mode | 0o111
	else:
		if mode & 0o111:
			newmode = mode & ~0o111
	if newmode:
		os.fchmod(fh.fileno(), newmode)

def _file_creation(fn, func):
	"""
	Perform func(), retry by recreating directory if failing
	"""
	parentdirs, _ = os.path.split(fn)
	try:
		func()
	except FileNotFoundError:
		os.makedirs(parentdirs, exist_ok=True)
		func()

def main():
	inst = _Server()
	inst.main()
