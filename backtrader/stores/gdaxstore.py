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

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass

import gdax
import requests.exceptions


class GDAXStream(gdax.WebsocketClient):
    def __init__(self, dataname, q, *args, **kwargs):
        super(gdax.WebsocketClient, self).__init__(*args, **kwargs)

        self.q = q
        self.dataname = dataname

    def on_open(self):
        self.url = "wss://ws-feed.gdax.com/"
        self.products = self.dataname

    def on_message(self, msg):
        if 'type' == 'match':
            self.q.put(msg)

    def on_close(self):
        pass


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


class GDAXStore(with_metaclass(MetaSingleton, object)):
    """Singleton class wrapping to control the connections to GDAX.
    """

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (
        ('key', ''),
        ('b64secret', ''),
        ('passphrase', ''),
        ('debug', False),  # account balance refresh timeout
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
        super(GDAXStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self.gdax = gdax.authenticated_client.AuthenticatedClient(self.p.key, self.p.b64secret, self.p.passphrase)
        self.gdax_public = gdax.public_client.PublicClient()

        self._debug = self.p.debug
        self._cash = 0.0
        self._value = 0.0
        self._evt_acct = threading.Event()

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
        if timeframe >= bt.TimeFrame.Seconds or timeframe <= bt.TimeFrame.Days:
            return True
        else:
            return False

    def candles(self, dataname, dtbegin, dtend, timeframe, compression, timeout=0.33):

        kwargs = locals().copy()
        kwargs.pop('self')
        kwargs['q'] = q = queue.Queue()
        t = threading.Thread(target=self._t_candles, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_candles(self, dataname, dtbegin, dtend, timeframe, compression, q, timeout=0.33):
        # GDAX imposed limit of 200 results/req
        limit = 200

        if not self.get_granularity(timeframe, compression):
            q.put(dict(code=2, msg="Unsupported time frame"))
            return

        if timeframe == bt.TimeFrame.Seconds:
            compression = compression
        elif timeframe == bt.TimeFrame.Minutes:
            compression = 60 * compression
        elif timeframe == bt.TimeFrame.Days:
            compression = 86400 * compression

        if dtbegin:
            ts_start = int((dtbegin - self._DTEPOCH).total_seconds())
        else:
            ts_start = int(_time.time()) - (limit * compression)

        if dtend:
            ts_end = int((dtend - self._DTEPOCH).total_seconds())
        else:
            ts_end = int(_time.time())

        time_delta = ts_end - ts_start

        if ts_end < ts_start:
            q.put(dict(code=2, msg="Start time in the future ?"))
            return

        slices = int(time_delta / compression)
        dt_slices = list()
        if slices > limit:
            # request too large and needs to be split (GDAX will only return 200 results/req)
            s_end = ts_start
            for t_sl in range(0, int(slices / limit)):
                s_start = s_end
                s_end = s_start + (compression * limit)
                dt_slices.append((datetime.utcfromtimestamp(s_start).isoformat(),
                                  datetime.utcfromtimestamp(s_end).isoformat()))
                s_end += compression
            if slices % limit > 0:
                dt_slices.append((datetime.utcfromtimestamp(s_end),
                                  datetime.utcfromtimestamp(ts_end).isoformat()))
        else:
            dt_slices.append((dtbegin.isoformat(), datetime.utcfromtimestamp(ts_end).isoformat()))

        try:
            for dt_start, dt_end in dt_slices:
                self.log(dataname, dt_start, dt_end, compression)
                response = self.gdax_public.get_product_historic_rates(dataname, start=str(dt_start), end=str(dt_end),
                                                                       granularity=compression)
                if isinstance(response, collections.Mapping) and 'message' in response.keys():
                    err = dict(code=2, message=response['message'], description="GDAX error")
                    self.log(err)
                    q.put(err)
                    break
                candles = sorted(response, key=lambda x: x[0])
                self.log(len(candles))
                for candle in candles:
                    q.put(candle)
                if len(dt_slices) > 1:
                    _time.sleep(timeout)  # throttling needed as GDAX imposes 3req/s max
        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                err = dict(code=3, message=str(e), description="Connection error")
                self.log(err)
                q.put(err)
                return
        except Exception as e:
            err = dict(code=1, message=str(e), description="Exception caught")
            self.log(err)
            q.put(err)
            q.put(None)
            return

        q.put({})  # end of transmission

    def streaming_prices(self, dataname, tmout=0.4):
        q = queue.Queue()
        kwargs = {'q': q, 'dataname': dataname, 'tmout': tmout}
        t = threading.Thread(target=self._t_streaming_prices, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_streaming_prices(self, dataname, q, tmout=0.4):
        while True:
            try:
                response = self.gdax_public.get_product_ticker(dataname)
                self.log("ticker", response['time'], response['bid'], response['ask'],
                         response['price'], response['size'])
                if isinstance(response, collections.Mapping) and 'message' in response.keys():
                    err = dict(code=2, message=response['message'], description="GDAX error")
                    self.log(err)
                    q.put(err)
                    break
                q.put(response)
                _time.sleep(tmout)   # throttling needed as GDAX imposes 3req/s max
            except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                self.log(str(e))
                err = dict(code=3, message=str(e), description="Connection error")
                q.put(err)
                break
            except (requests.exceptions.TooManyRedirects, Exception) as e:
                self.log(str(e))
                err = dict(code=1, message=str(e), description="Exception caught")
                q.put(err)
                break

        # wsClient = GDAXStream(dataname, q)
        # wsClient.start()
        # wsClient.close()
