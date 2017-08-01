#!/usr/bin/env python3
import config
from app import MeterDigitizer

if __name__ == '__main__':
    args = config.parse_args();
    app = MeterDigitizer()
    if args.fcgi:
        import sys
        sys.exit("FastCGI mode is not yet supported")
        app.run(host=args.http_bind, port=args.http_port)
