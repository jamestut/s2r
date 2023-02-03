from subprocess import Popen, PIPE
import os
import struct
from enum import Enum
import shared
from PythonLib.MyBytesIO import BIO

MAX_BUFF_SZ = 1_048_576

class BulkOpRetType(Enum):
	GENERIC = 1
	OPENFD = 2

class _Client:
	def __init__(self, fout, fin, remotecwd):
		self._fout = fout
		self._fin = fin
		self._buff = BIO(MAX_BUFF_SZ)
		self._buffman = shared.BuffManager(self._buff)
		self._maxofd = None
		self._svrmaxbuff = None
		self._hasbulkqueue = False
		self._enqueued_bulkops_rets = []
		self._enqueued_writes = 0

		self._negotiate(remotecwd)

	def _begin_msg(self, msgtype):
		self._buff.seek(0)
		self._buff.set_limit(self._svrmaxbuff) # remove limit
		self._buffman.begin_msg(msgtype)

	def _send_msg(self):
		self._buffman.end_msg()
		self._fout.write(self._buffman.getbuffer())

	def _recv_msg(self):
		cmd = shared.MsgType(shared.read_all(self._fin, 1)[0])
		arglen = struct.unpack("=I", shared.read_all(self._fin, 4))[0]
		if arglen > len(self._buff):
			raise RuntimeError("Server response exceeded buffer size")
		self._buff.set_limit(arglen)
		self._buff.seek(0)
		shared.readinto_all(self._fin, self._buff.getbuffer())
		return cmd

	def _send_and_recv_msg(self, expectedcmd=None):
		"""shortcut for self._send_msg and self._recv_msg"""
		self._send_msg()
		cmd = self._recv_msg()
		if expectedcmd is not None:
			if cmd != expectedcmd:
				raise RuntimeError(f"Unexpected response {cmd}, expected {expectedcmd}")
		return cmd

	def _negotiate(self, remotecwd):
		# check server version
		self._begin_msg(shared.MsgType.VERSION)
		self._send_and_recv_msg(shared.MsgType.VERSION_RESP)
		server_ver = struct.unpack("=I", self._buff.read(4))[0]
		if server_ver != 1:
			raise RuntimeError(f"Unknown server version: {server_ver}")

		# get server limits
		self._begin_msg(shared.MsgType.REQ_LIMIT)
		self._send_and_recv_msg(shared.MsgType.LIMIT_RESP)
		self._maxofd, self._svrmaxbuff = struct.unpack("=II", self._buff.read(8))
		self._svrmaxbuff = min(self._svrmaxbuff, MAX_BUFF_SZ)

		# chdir
		self._begin_msg(shared.MsgType.CHDIR)
		remotecwd = remotecwd.encode('utf8')
		self._buffman.append_bytes(remotecwd)
		self._send_and_recv_msg(shared.MsgType.GEN_RESULT)
		errnoval = struct.unpack("=H", self._buff.read(2))[0]
		if errnoval != 0:
			raise _gen_oserror(errnoval)

	def _init_bulkop_queue(self):
		if not self._hasbulkqueue:
			self._begin_msg(shared.MsgType.BULKOP_BEGIN)
			self._hasbulkqueue = True
			self._enqueued_writes = 0
			self._enqueued_bulkops_rets.clear()

	def _enqueue_bulk_open(self, rettype, func):
		"""
		This function executes 'func' (which is intended to fill the buffer) and
		will reset buffer's position 'func' raises BufferError exception.
		returns:
			True if data is enqueued
			False if data is not enqueued
		"""
		if not self._hasbulkqueue:
			raise RuntimeError("Need to initialize bulk queue first")
		undopos = self._buff.tell()
		try:
			func()
		except BufferError:
			self._buff.seek(undopos)
			return False
		self._enqueued_bulkops_rets.append(rettype)
		return True

	def queue_delete(self, fn):
		self._init_bulkop_queue()

		def _enqueue():
			self._buffman.append_byte(shared.OpType.DELETE.value)
			fnb = fn.encode('utf8')
			self._buffman.append_huint(len(fnb))
			self._buffman.append_bytes(fnb)

		return self._enqueue_bulk_open(BulkOpRetType.GENERIC, _enqueue)

	def queue_upload(self, fn, target):
		self._init_bulkop_queue()
		fn = fn.encode('utf8')

		if isinstance(target, str):
			# symlink
			def _enqueue():
				self._buffman.append_byte(shared.OpType.SYMLINK.value)
				symlink_target = target.encode('utf8')
				self._buffman.append_huint(len(fn))
				self._buffman.append_bytes(fn)
				self._buffman.append_huint(len(symlink_target))
				self._buffman.append_bytes(symlink_target)
			rettype = BulkOpRetType.GENERIC
		else:
			# regular file, subject to OFD limitations
			if self._enqueued_writes >= self._maxofd:
				return False
			def _enqueue():
				# target is a bool: True to make it executable
				self._buffman.append_byte(shared.OpType.WRITE.value)
				self._buffman.append_byte(1 if target else 0)
				self._buffman.append_huint(len(fn))
				self._buffman.append_bytes(fn)
				self._enqueued_writes += 1
			rettype = BulkOpRetType.OPENFD

		return self._enqueue_bulk_open(rettype, _enqueue)

	def run_bulk_queue(self):
		if not self._hasbulkqueue:
			raise RuntimeError("Need to enqueue an operation first")
		self._send_and_recv_msg(shared.MsgType.BULKOP_RESULTS)
		self._hasbulkqueue = False
		# convert result
		return [(rettype, _convert_bulkopen_result(rettype, self._buff))
			for rettype in self._enqueued_bulkops_rets]

	def close_bulk_queue(self):
		self._begin_msg(shared.MsgType.BULKOP_CLOSE)
		self._send_and_recv_msg(shared.MsgType.BULKOP_CLOSE_RESULTS)
		# convert result
		return [_convert_bulkopen_result(BulkOpRetType.OPENFD, self._buff)
			for _ in range(self._enqueued_writes)]

	def upload_file(self, rfd, fh):
		total = 0 # total bytes written
		while True:
			self._begin_msg(shared.MsgType.CHUNK)
			self._buffman.append_uint(rfd)
			opbuff = self._buff.getbuffer()[self._buff.tell():]
			rd = fh.readinto(opbuff)
			if not rd: # EOF
				break
			self._buff.seek(rd, 1)
			# there is no reply while sending chunks to minimize round-trip
			self._send_msg()
			total += rd
		return total

class _OpQueue:
	def __init__(self, client):
		self._client = client
		self._enqueued_ops = [] # list of (relative file name, operations data)
		self._open_fds = {} # dict {remote fd: relative file name}

	def _do_process_queue(self):
		"""Do not call this function directly. Call self.do_process_queue instead."""
		res = self._client.run_bulk_queue()
		if len(res) != len(self._enqueued_ops):
			raise RuntimeError("Server sent invalid response for bulk open")

		for i in range(len(self._enqueued_ops)):
			op = self._enqueued_ops[i]
			rettype, retval = res[i]
			if op[1] is None:
				# delete request
				if rettype != BulkOpRetType.GENERIC:
					raise RuntimeError("Invalid response for delete request")
				if retval[0] != 0:
					print(f"Error deleting '{op[0]}': {os.strerror(retval[0])}")
					return False
				print(f"Deleted '{op[0]}'")
			elif isinstance(op[1], str):
				# symlink request
				if rettype != BulkOpRetType.GENERIC:
					raise RuntimeError("Invalid response for symlink request")
				if retval[0] != 0:
					print(f"Error creating symlink '{op[0]}': {os.strerror(retval[0])}")
					return False
				print(f"Created symlink '{op[0]}' -> '{op[1]}'")
			else:
				# regular file
				if rettype != BulkOpRetType.OPENFD:
					raise RuntimeError("Invalid response for file write request")
				fd, errnoval = retval
				if fd < 0:
					print(f"Error opening file '{op[0]}' for write: {os.strerror(errnoval)}")
					return False
				# some files are open only to update permission: we won't register
				# their open fd here
				if op[1]:
					self._open_fds[fd] = op[0]
				else:
					print(f"Permission updated for '{op[0]}'")

		# OK
		return True

	def _do_upload_files(self):
		"""Do not call this function directly. Call self.do_process_queue instead."""
		for rfd, fn in self._open_fds.items():
			print(f"Uploading '{fn}' ...")
			with open(fn, 'rb', buffering=0) as f:
				self._client.upload_file(rfd, f)

		for rfd, errnoval in self._client.close_bulk_queue():
			if rfd not in self._open_fds:
				# permssion update only. skip.
				continue
			fn = self._open_fds[rfd]
			if errnoval == 0:
				print(f"File '{fn}' uploaded")
			else:
				print(f"Error uploading '{fn}': {os.strerror(errnoval)}")
				return False

		# OK
		return True

	def do_process_queue(self):
		"""Perform additional checks as well as cleaning the queue once finished"""
		try:
			self._open_fds.clear()
			if not self._do_process_queue():
				return False
			if not self._do_upload_files():
				return False
			return True
		finally:
			self._enqueued_ops.clear()

	def enqueue(self, fn, update_data, **kwargs):
		"""
		args:
			fn: file name
			update_data:
				None for deletion
				str for symlink (contains target path)
				bool for file addition (True to upload content, False to just open)
			executable:
				Only for normal file. Set to True to make remote file executable.
		"""
		if update_data is None:
			rs = self._client.queue_delete(fn)
		else:
			rs = self._client.queue_upload(fn, kwargs['executable'])

		if rs:
			# enqueue successful
			self._enqueued_ops.append((fn, update_data))
			return True

		# if we're here, queue buffer is full, process the existing queue
		if not self.do_process_queue():
			# queue processing failed. tell caller
			return False
		# at this point, queue processing was successful, retry enqueueing
		# this queue again
		return self.enqueue(fn, update_data, **kwargs)

def _gen_oserror(errnoval):
	return OSError(errnoval, os.strerror(errnoval))

def _convert_bulkopen_result(rettype, bio):
	if rettype == BulkOpRetType.GENERIC:
		return struct.unpack("=H", bio.read(2))
	if rettype == BulkOpRetType.OPENFD:
		return struct.unpack("=IH", bio.read(6))
	raise RuntimeError("Unknown rettype")

def run_sync(sw, newstate, to_delete, to_update):
	if not to_delete and not to_update:
		print("Nothing to be done!")
		return True
	p = Popen(sw['command'], stdout=PIPE, stdin=PIPE)

	client = _Client(p.stdin.raw, p.stdout.raw, sw['remotecwd'])
	opqueue = _OpQueue(client)

	for k in to_delete:
		if not opqueue.enqueue(k, None):
			return False
	for k, v in to_update.items():
		if not opqueue.enqueue(k, v, executable=newstate[k][0]):
			return False
	# the remaining operations
	return opqueue.do_process_queue()
