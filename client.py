import sys
import os
import json
from clientcomm import run_sync

DEFAULT_STATE_FILE = '.s2rstate.json'

def _recursive_scan(path, result):
	"""
	'result' is a dict of {relative path name: [target, mtime]}
		'target' would be a string for symlinks.
		On normal file, 'target' is a bool that contains whether the target file
		is executable or not.
	The 'path' argument suplied in this function must be '.'.
	"""
	for de in os.scandir(path):
		include = False
		is_symlink = de.is_symlink()
		if not is_symlink:
			if de.is_dir():
				_recursive_scan(os.path.join(path, de.name), result)
			elif de.is_file():
				include = True
			# ignore non-file
		else:
			# include (but do not traverse) symlinks
			include = True

		if include:
			st = de.stat(follow_symlinks=False)
			mtime = max(st.st_ctime_ns, st.st_mtime_ns)
			pth = os.path.join(path, de.name)[2:] # remove leading './'
			# contains either a boolean indicating executable stat for regular file
			# or symlink target for symlink
			info = None
			if is_symlink:
				info = os.readlink(pth)
			else:
				info = bool(st.st_mode & 0o111)
			result[pth] = (info, mtime)

def recursive_scan(statefile):
	ret = {}
	_recursive_scan(".", ret)
	# do not include state file for syncing!
	if statefile in ret:
		del ret[statefile]
	return ret

def _gen_state(args, empty):
	sf = None
	try:
		try:
			os.stat(args.statefile)
			sf = open(args.statefile, 'a+')
			sf.seek(0)
			sw = json.load(sf)
		except FileNotFoundError:
			sw = {
				"command": [],
				"remotecwd": None,
				"data": None
			}

		if not empty:
			sw["data"] = recursive_scan(args.statefile)
		else:
			sw["data"] = {}

		if not sf:
			sf = open(args.statefile, 'w')
		else:
			sf.truncate(0)
			sf.seek(0)
		json.dump(sw, sf)
	finally:
		if sf:
			sf.close()

	return 0

def _do_sync(args):
	try:
		with open(args.statefile, 'r') as f:
			sw = json.load(f)
	except FileNotFoundError:
		print("State file not found. Generate using the 'genstate' command.",
			file=sys.stderr)
		return 1

	if not args.dryrun:
		if not sw['command']:
			print("Please specify command to start the remote server.",
				file=sys.stderr)
			print(f"Put the command argv as an array in the 'command' entry in "\
					f"'{args.statefile}'.", file=sys.stderr)
			return 1
		if not sw['remotecwd']:
			print("Please specify target remote folder.",
				file=sys.stderr)
			print(f"Put the target remote folder in the 'remotecwd' entry in "\
					f"'{args.statefile}'.", file=sys.stderr)
			return 1

	oldstate = sw['data']
	newstate = recursive_scan(args.statefile)

	to_delete = [k for k in oldstate if k not in newstate]
	# dict of: {filename: data}
	# if data is string, then it is a symlink
	# if data is a bool, if True, then initiate upload. If False, open only
	# (to update file attributes).
	to_update = {}

	# additional criteria for update/delete
	for k, v in newstate.items():
		if k not in oldstate:
			info, _ = v
			if isinstance(info, bool):
				# new file, always upload
				to_update[k] = True
			elif isinstance(info, str):
				# symlink
				to_update[k] = info
			else:
				raise TypeError("Unknown stat item")
			continue

		oldv = oldstate[k]
		if isinstance(v[0], bool) and isinstance(oldv[0], bool):
			# both are files
			if v[1] > oldv[1]:
				# ctime/mtime is newer: upload
				to_update[k] = True
			elif v[0] != oldv[0]:
				# executable state changed: just open and chmod
				to_update[k] = False
		else:
			# either or both of them are symlinks
			if v[0] != oldv[0]:
				# If we're here, either symlink contents changed, file becomes
				# symlink (or vice versa), nonexecutable becomes executable (or
				# vice versa).
				to_update[k] = v[0] if isinstance(v[0], str) else True

	if args.dryrun:
		_print_dryrun(to_delete, to_update)
		return 0

	# do the real thing
	if not run_sync(sw, newstate, to_delete, to_update):
		# failed
		print("Sync failed.")
		return 1

	# OK! don't forget update state file
	sw['data'] = newstate
	with open(args.statefile, 'w') as f:
		json.dump(sw, f)
	print("Sync successful.")
	return 0

def _print_dryrun(to_delete, to_update):
	if not to_delete:
		print("(No remote files will be deleted)")
	else:
		print("These files will be deleted on the remote server:")
		for k in to_delete:
			print(f"  {k}")
	print()
	if not to_update:
		print("(No remote files will be uploaded)")
	else:
		print("These files will be uploaded to the remote server:")
		for k in to_update:
			print(f"  {k}")

def populate_subparsers(sp):
	ssp = sp.add_subparsers(title="client commands", dest="clientcmd", required=True)
	parser_genstate = ssp.add_parser("genstate",
		help="Generate state file where all files in this current folder is "\
		"regarded as already synced.")
	parser_genemptystate = ssp.add_parser("genemptystate",
		help="Generate state file where all files in this current folder is "\
		"regarded as new.")
	parser_sync = ssp.add_parser("sync",
		help="Perform synchronisation")

	# add common options
	for csp in [parser_genstate, parser_genemptystate, parser_sync]:
		csp.add_argument("--cwd",
			help="Directory to be synced (default: current directory).")
		csp.add_argument("--statefile",
			help=f"s2r client's state file name (default: '{DEFAULT_STATE_FILE}')",
			default=DEFAULT_STATE_FILE)

	# sync specific options
	parser_sync.add_argument("--dryrun", action="store_true",
		help="List files that will be deleted and updated. No actions will be taken.")

def main(args):
	if args.cwd is not None:
		os.chdir(args.cwd)
	if '/' in args.statefile or args.statefile in {'', '.', '..'}:
		assert ValueError("Invalid state file name")

	if args.clientcmd == 'genstate':
		ret = _gen_state(args, False)
	elif args.clientcmd == 'genemptystate':
		ret = _gen_state(args, True)
	elif args.clientcmd == 'sync':
		ret = _do_sync(args)

	sys.exit(ret)
