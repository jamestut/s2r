import sys
import argparse

import server
import client

def main():
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers(title='modes', dest='mode', required=True)
	parser_client = subparsers.add_parser('client',
		help='Run program locally to sync local files to remote server')
	parser_server = subparsers.add_parser('server',
		help='To be run by client. Receives client commands and execute them.')
	client.populate_subparsers(parser_client)

	args = parser.parse_args()
	if args.mode == 'client':
		client.main(args)
	elif args.mode == 'server':
		server.main()

if __name__ == "__main__":
	sys.exit(main())
