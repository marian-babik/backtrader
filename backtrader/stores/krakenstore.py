#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2017 Marian Babik
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
from datetime import datetime
import time as _time
import threading
import urllib
import hashlib
import hmac
import base64
import urlparse
import requests

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass


class KrakenAPI(object):
    def __init__(self, key=None, secret=None, debug=False):
        self.key = key
        self.secret = secret
        self.uri = "https://api.kraken.com"
        self._debug = debug

    def log(self, *args):
        if self._debug:
            print(*args)

    def request(self, path, **kwargs):
        if 'private' in path:
            assert self.key and self.secret
            kwargs['nonce'] = int(1000 * _time.time())
            post_data = urllib.urlencode(kwargs)
            message = path + hashlib.sha256(str(kwargs["nonce"]) + post_data).digest()
            signature = hmac.new(base64.b64decode(self.secret), message, hashlib.sha512)
            headers = {
                'API-Key': self.key,
                'API-Sign': base64.b64encode(signature.digest())
            }
            response = requests.post(urlparse.urljoin((self.uri, path), data=kwargs), headers=headers)
        else:
            response = requests.get(urlparse.urljoin(self.uri, path), params=kwargs)

        # self.log(response.url, response.status_code, len(response.text))
        response.raise_for_status()
        p_res = response.json()
        if "error" in p_res.keys():
            error = p_res['error']
        return p_res, error


class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class KrakenStore(with_metaclass(MetaSingleton, object)):
    """Singleton class wrapping to control the connections to GDAX.
    """

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (
        ('key', ''),
        ('secret', ''),
        ('debug', False),
        ('timeoffset', True),  # Use offset to server for timestamps
        ('timerefresh', 60.0),  # How often to refresh the timeoffset
    )

    _DTEPOCH = datetime(1970, 1, 1)

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(KrakenStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._debug = self.p.debug
        self._cash = 0.0
        self._value = 0.0
        self._evt_acct = threading.Event()

        self._time_diff = 0.0
        self._lock_tmoffset = threading.Lock()
        self.sync_time()

        self.kraken = KrakenAPI(key=self.p.key, secret=self.p.secret, debug=self._debug)

    def start(self, data=None, broker=None):
        # Datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            #self.streaming_events()
            #self.broker_threads()

    def log(self, *args):
        if self._debug:
            print(*args)

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            pass
            # self.q_ordercreate.put(None)
            # self.q_orderclose.put(None)
            # self.q_account.put(None)

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Return the pending "store" notifications"""
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]

    def get_cash(self):
        return self._cash

    def get_value(self):
        return self._value

    def sync_time(self):
        if not self.p.timeoffset:
            return
        threading.Timer(self.p.timerefresh, self._get_time_offset).start()

    def _get_time_offset(self):
        resp, error = self.kraken.request("/0/public/Time")
        if 'result' in resp.keys() and 'unixtime' in resp['result'].keys():
            with self._lock_tmoffset:
                self._time_diff = int(_time.time()) - resp['result']['unixtime']
                self.log("TimeSync: server time (%d) - offset %f" % (resp['result']['unixtime'], self._time_diff))
        else:
            self.log("Failed to sync time with the server, got (%s) %s" % (resp, error))

    @staticmethod
    def get_granularity(timeframe, compression):
        # Kraken API only supports the following granularity in minutes
        # 1 (default), 5, 15, 30, 60, 240, 1440, 10080, 21600
        if timeframe == bt.TimeFrame.Minutes and compression in [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]:
            return True
        else:
            return False

    def candles(self, dataname, dtbegin, dtend, timeframe, compression):

        kwargs = locals().copy()
        kwargs.pop('self')
        kwargs['q'] = q = queue.Queue()
        t = threading.Thread(target=self._t_candles, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_candles(self, dataname, dtbegin, dtend, timeframe, compression, q):

        if not self.get_granularity(timeframe, compression):
            q.put(dict(code=2, msg="Unsupported time frame"))
            return

        if dtbegin:
            ts_start = int((dtbegin - self._DTEPOCH).total_seconds())
        else:
            ts_start = int(_time.time()) - (compression * 60 * 200)  # fetch last 200 values

        # KRAKEN API only supports dtbegin in historical prices
        # only single request can be made based in timeframe.Minutes/Compression
        try:
            self.log(dataname, compression)
            response, error = self.kraken.request("/0/public/OHLC", pair=dataname, since=ts_start,
                                                  interval=compression)
            if error:
                err = dict(code=2, message=error, description="KRAKEN error")
                q.put(err)
                return
            if dataname not in response['result'].keys():
                err = dict(code=1, message="OHLC for {0} not found" % dataname, description="Pair error")
                q.put(err)
                return
            for candle in response['result'][dataname]:
                q.put(candle)
        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                self.log(str(e))
                err = dict(code=3, message=str(e), description="Connection error")
                q.put(err)
                return
        except Exception as e:
            err = dict(code=1, message=str(e), description="Exception caught")
            self.log(str(e))
            q.put(err)
            q.put(None)
            return

        q.put({})  # end of transmission

    def streaming_prices(self, dataname, tmout=2.0, compression=None, candles=False):
        q = queue.Queue()
        kwargs = {'q': q, 'dataname': dataname, 'tmout': tmout, 'compression': compression}
        if not candles:
            t = threading.Thread(target=self._t_streaming_prices, kwargs=kwargs)
        else:
            t = threading.Thread(target=self._t_streaming_candles, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_streaming_candles(self, dataname, q, tmout=2.0, compression=1):
        ts_start = int(_time.time()) - (60 * compression)
        while True:
            try:
                response, error = self.kraken.request("/0/public/OHLC", pair=dataname, since=ts_start,
                                                          interval=compression)
                if error:
                    err = dict(code=2, message=error, description="KRAKEN error")
                    q.put(err)
                    return
                if dataname not in response['result'].keys():
                    err = dict(code=1, message="OHLC for {0} not found" % dataname, description="Pair error")
                    q.put(err)
                    return
                ts_start = int(response['result']['last'])
                for candle in response['result'][dataname][:-1]:
                    self.log('ticker', candle)
                    q.put(candle)
                _time.sleep(tmout)
            except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                self.log(str(e))
                err = dict(code=3, message=str(e), description="Connection error")
                q.put(err)
                break
            except Exception as e:
                err = dict(code=1, message=str(e), description="Exception caught")
                self.log(str(e))
                q.put(err)
                q.put(None)
                break

    def _t_streaming_prices(self, dataname, q, tmout=2.0):
        if tmout is not None:
            _time.sleep(tmout)

        while True:
            try:
                resp, error = self.kraken.request("/0/public/Ticker", pair=dataname)
                ticker_time = int(_time.time()) + self._time_diff   # API provides no timestamp
                if error:
                    self._t_streaming_err(2, error, "KRAKEN API error", q)
                    break
                if dataname not in resp['result'].keys():
                    self._t_streaming_err(1, "Ticker for {0} not found" % dataname, "Pair error", q)
                    break
                ticker = resp['result'][dataname]
                ticker['time'] = ticker_time
                self.log('ticker', ticker['a'][0], ticker['b'][0], ticker['c'][0], ticker['c'][1])
                q.put(ticker)
                _time.sleep(tmout)   # throttling needed as KRAKEN imposes ~ 1req/2s
            except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                self._t_streaming_err(3, str(e), "KRAKEN connection error", q)
                return
            except Exception as e:
                import traceback
                traceback.print_exc()
                self._t_streaming_err(1, str(e), "General exception", q)
                break

    def _t_streaming_err(self, code, error, descr, q):
        err = dict(code=code, message=error, description=descr)
        q.put(err)
        self.log(err)


