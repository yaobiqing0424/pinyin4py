# -*- coding: utf-8 -*-

import sys
import BaseHTTPServer
import urlparse
import urllib
import pinyin

converter = pinyin.Converter()

class WebHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):

        rt = urlparse.urlparse(self.path)
        qs = urlparse.parse_qs(rt.query)

        if 'text' not in qs:
            self.send_error(500, 'Missing parameter: text')
            return
        text = urllib.unquote(qs['text'][0])

        if 'fmt' in qs:
            fmt = qs['fmt'][0]
            if fmt not in ['df', 'tm', 'tn', 'ic', 'fl']:
                self.send_error(500, 'Invalid parameter value: fmt=%s' % fmt)
                return
        else:
            fmt = 'df'

        if 'sc' in qs:
            sc = qs['sc'][0]
            if sc == 'true':
                sc = True
            elif sc == 'false':
                sc = False
            else:
                self.send_error(500, 'Invalid parameter value: sc=%s' % sc)
                return
        else:
            sc = True

        if 'pp' in qs:
            pp = qs['pp'][0]
            if pp == 'true':
                pp = True
            elif pp == 'false':
                pp = False
            else:
                self.send_error(500, 'In valid parameter value: pp=%s' % pp)
                return
        else:
            pp = False

        if 'fuzzy' in qs:
            fuzzy = qs['fuzzy'][0]
            try:
                fuzzy = int(fuzzy)
                if fuzzy < 0:
                    raise ValueError
            except ValueError:
                self.send_error(500, 'Invalid parameter value: fuzzy=%s' % fuzzy)
                return
        else:
            fuzzy = 0

        global converter
        try:
            res = converter.convert(text, fmt, sc, pp, fuzzy)
        except Exception as e:
            self.send_error(500, 'Fail to convert: %s' % e)
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain;charset=utf-8')
        self.end_headers()
        self.wfile.write(res)


def main():
    BaseHTTPServer.HTTPServer((sys.argv[1], int(sys.argv[2])), WebHandler).serve_forever()

if __name__ == '__main__':
    main()
