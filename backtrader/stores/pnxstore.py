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
import requests

import poloniex

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass


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


class PNXStore(with_metaclass(MetaSingleton, object)):
    """Singleton class wrapping to control the connections to Poloniex.
    """

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (
        ('key', ''),
        ('secret', ''),
        ('debug', False),
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
        super(PNXStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._debug = self.p.debug
        self._cash = 0.0
        self._value = 0.0
        self._evt_acct = threading.Event()

        self.pnx = poloniex.Poloniex(apikey=self.p.key, secret=self.p.secret)

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

    @staticmethod
    def get_granularity(timeframe, compression):
        # PNX API only supports the following granularity in seconds
        if timeframe == bt.TimeFrame.Seconds and compression in [300, 900, 1800, 7200, 14400, 86400]:
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
        limit = 50000
        if not self.get_granularity(timeframe, compression):
            q.put(dict(code=2, msg="Unsupported time frame"))
            return

        if dtbegin:
            ts_start = int((dtbegin - self._DTEPOCH).total_seconds())
        else:
            ts_start = int(_time.time()) - (compression * 50000)  # fetch last 50k values

        if dtend:
            ts_end = int((dtend - self._DTEPOCH).total_seconds())
        else:
            ts_end = int(_time.time())

        # max of 50k entries can only be returned for candles
        time_delta = ts_end - ts_start

        if ts_end < ts_start:
            q.put(dict(code=2, msg="Start time in the future ?"))
            return

        slices = int(time_delta / compression)
        dt_slices = list()
        if slices > limit:
            # request too large and needs to be split
            s_end = ts_start
            for t_sl in range(0, int(slices / limit)):
                s_start = s_end
                s_end = s_start + (compression * limit)
                dt_slices.append((s_start, s_end))
                s_end += compression
            if slices % limit > 0:
                dt_slices.append((s_end, ts_end))
        else:
            dt_slices.append((ts_start, ts_end))

        try:
            for dt_start, dt_end in dt_slices:
                self.log(dataname, dt_start, dt_end, compression)
                response = self.pnx.returnChartData(dataname, compression, ts_start, ts_end)
            if not response:
                err = dict(code=1, message="OHLC for {0} not found" % dataname, description="Pair error")
                q.put(err)
                return
            # for candle in response.text['result'][:-1]:
            for candle in response:
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

    def _t_streaming_candles(self, dataname, q, tmout=2.0, compression=1800):
        ts_start = int(_time.time()) - (2 * compression)
        ts_end = 9999999999

        while True:
            try:
                response = self.pnx.returnChartData(dataname, compression, ts_start, ts_end)
                if not response:
                    err = dict(code=1, message="OHLC for {0} not found" % dataname, description="Pair error")
                    q.put(err)
                    return
                ts_start = int(response[-1]['date'])
                #for candle in response[:-1]:
                for candle in response:
                    self.log('ticker', candle)
                    q.put(candle)
                # throttling
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
                response = self.pnx.returnTicker()
                ticker_time = int(_time.time())   # API provides no timestamp and no time difference to server
                if not response.text:
                    self._t_streaming_err(2, "Empty response", "PNX API error", q)
                    break
                if dataname not in response.text.keys():
                    self._t_streaming_err(1, "Ticker for {0} not found" % dataname, "Pair error", q)
                    break
                ticker = response.text[dataname]
                ticker['time'] = ticker_time
                self.log('ticker', ticker['last'], ticker['lowestAsk'], ticker['highestBid'], ticker['volume'])
                q.put(ticker)
                # throttling not needed
            except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                self._t_streaming_err(3, str(e), "PNX connection error", q)
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


