from argparse import ArgumentParser
import asyncio

from .server import Server


try:
    import pkg_resources
    VERSION = pkg_resources.get_distribution("binlog").version
except:
    VERSION = "UNKNOWN"


def main():
    print("binlog v%s\n" % VERSION)
    parser = ArgumentParser()
    parser.add_argument("environment", nargs=1)
    parser.add_argument("socket", nargs=1)
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    s = Server(args.environment[0], args.socket[0])

    s.run()


if __name__ == '__main__':
    main()
