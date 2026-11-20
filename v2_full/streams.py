"""Public WS adapters for Gate / Bitget / MEXC using sequenced books and real fills.
No secrets or orders. Network calls only when this program is explicitly run live.
websocket-client is the only non-stdlib runtime dependency.
"""
from __future__ import annotations
from collections import defaultdict
from decimal import Decimal
import json, threading, time, urllib.request, urllib.parse
from .core import VenueState, Gap, stamp
from .mexc_pb import decode as decode_mexc, DecodeError

GATE='wss://api.gateio.ws/ws/v4/'
BITGET='wss://ws.bitget.com/v3/ws/public'
MEXC='wss://wbs-api.mexc.com/ws'

def http_json(url,params=None,timeout=12):
    u=url+('?' +urllib.parse.urlencode(params) if params else '')
    req=urllib.request.Request(u,headers={'Accept':'application/json','User-Agent':'RadarV2-LiveEvidence/1.0'})
    with urllib.request.urlopen(req,timeout=timeout) as r:
        return json.load(r)

class StreamCapture:
    def __init__(self,config,identity=None):
        self.markets={}
        self.pairs={}
        self.locks=defaultdict(threading.RLock)
        self.gate_buffer=defaultdict(list)
        self.mexc_buffer=defaultdict(list)
        self.mexc_resyncing=set()
        self.gate_snapshot_loading=set()
        self.failures=[]
        self.ws_clients={}
        self.repair_attempts={}
        identity=identity or {}
        for x in config['venues']:
            if x['venue'] not in ('gate','bitget','mexc') or not x.get('pair'):continue
            coin,venue,pair=x['coin'],x['venue'],x['pair']
            v=VenueState(coin,venue,pair,identity.get(coin+'|'+venue,'UNVERIFIED_SYMBOL_MATCH'))
            self.markets[(venue,pair)]=v
            self.pairs[(venue,pair)]=coin
    def _get(self,venue,pair):return self.markets.get((venue,pair))
    def disconnected(self,venue,reason):
        for (v,_),s in self.markets.items():
            if v==venue:s.invalidate(reason)
    def gate_message(self,msg):
        if msg.get('event')=='error':
            self.disconnected('gate','EXCHANGE_ERROR:'+str(msg)[:120]);return
        if msg.get('event')!='update':return
        channel=msg.get('channel');data=msg.get('result')
        if isinstance(data,list):entries=data
        elif isinstance(data,dict):entries=[data]
        else:return
        for item in entries:
            pair=item.get('currency_pair') if channel=='spot.trades' else item.get('s')
            s=self._get('gate',pair)
            if s is None:continue
            with self.locks[('gate',pair)]:
                try:
                    if channel=='spot.trades':
                        # Gate explicitly documents `side` as taker aggressor.
                        s.tape.add(item.get('id_market') or item['id'],int(Decimal(str(item['create_time_ms']))),
                                   item['side'],item['price'],item['amount'])
                    elif channel=='spot.order_book_update':
                        if not s.book.synced:
                            self.gate_buffer[pair].append(item)
                            if len(self.gate_buffer[pair])>25000:
                                self.gate_buffer[pair].clear();s.book.invalid('BOOK_BUFFER_OVERFLOW')
                        else:s.book.update(item.get('b',[]),item.get('a',[]),int(item['U']),int(item['u']),int(item['t']),'gate')
                except (ValueError,Gap,KeyError,TypeError) as e:
                    s.invalidate('GATE_PARSE_OR_SEQUENCE_GAP:'+str(e)[:100])
    def seed_gate_book(self,pair,snapshot):
        """Seed from REST snapshot then apply buffered websocket diffs with Gate's U/u rules.
        Call from a separate thread while WS receiver buffers messages.
        """
        s=self._get('gate',pair)
        if not s:return
        with self.locks[('gate',pair)]:
            try:
                base=int(snapshot['id'])
                s.book.snapshot(snapshot['bids'],snapshot['asks'],base,stamp())
                for item in self.gate_buffer[pair]:
                    if int(item['u'])<=s.book.seq:continue
                    s.book.update(item.get('b',[]),item.get('a',[]),int(item['U']),int(item['u']),int(item['t']),'gate')
                self.gate_buffer[pair].clear()
            except (KeyError,ValueError,Gap) as e:
                s.book.invalid('GATE_REST_WS_REPLAY_FAILED:'+str(e)[:80])
                self.gate_buffer[pair].clear()
    def bitget_message(self,msg):
        if msg.get('event')=='error':
            self.disconnected('bitget','BITGET_CHANNEL_ERROR:'+str(msg)[:100]);return
        arg=msg.get('arg') or {}
        if arg.get('instType') not in ('spot','SPOT'):return
        pair=arg.get('symbol')
        s=self._get('bitget',pair)
        if not s:return
        topic=arg.get('topic');action=msg.get('action');data=msg.get('data') or []
        if not isinstance(data,list):return
        with self.locks[('bitget',pair)]:
            for item in data:
                try:
                    if topic=='publicTrade':
                        if action=='snapshot':continue  # Historical trade replay, not new tape
                        if action!='update':continue
                        s.tape.add(item['i'],item['T'],item['S'],item['p'],item['v'])
                    elif topic=='books50':
                        if action=='snapshot':
                            s.book.snapshot(item['b'],item['a'],item['seq'],item['ts'])
                        elif action=='update':
                            s.book.update(item.get('b',[]),item.get('a',[]),int(item['pseq']),int(item['seq']),int(item['ts']),'bitget')
                except (KeyError,ValueError,Gap,TypeError) as e:
                    s.invalidate('BITGET_PARSE_OR_SEQUENCE_GAP:'+str(e)[:100])
    def mexc_message(self,b):
        try:msg=decode_mexc(b)
        except DecodeError:return
        pair=msg['pair'];s=self._get('mexc',pair)
        if not s:return
        with self.locks[('mexc',pair)]:
            try:
                if msg['type']=='trade':
                    for tr in msg['deals']:
                        s.tape.add(tr['id'],tr['ms'],tr['side'],tr['price'],tr['quantity'])
                elif msg['type']=='depth':
                    if s.book.synced:
                        s.book.update(msg['bids'],msg['asks'],msg['fromVersion'],msg['toVersion'],msg['send_ms'],'mexc')
                    else:
                        self.mexc_buffer[pair].append(msg)
                        if len(self.mexc_buffer[pair])>25000:
                            self.mexc_buffer[pair].clear();s.book.invalid('MEXC_BUFFER_OVERFLOW')
            except (KeyError,ValueError,Gap,TypeError) as e:
                s.invalidate('MEXC_MESSAGE_GAP:'+str(e)[:100])
    def seed_mexc(self,pair,snapshot):
        s=self._get('mexc',pair)
        if not s:return
        with self.locks[('mexc',pair)]:
            try:
                seq=int(snapshot['lastUpdateId'])
                s.book.snapshot(snapshot['bids'],snapshot['asks'],seq,stamp())
                for msg in self.mexc_buffer[pair]:
                    if msg['toVersion']<=s.book.seq:continue
                    # First replayed message MAY overlap snapshot sequence, as official docs allow.
                    if msg['fromVersion']>s.book.seq+1:
                        raise Gap('MEXC_REST_WS_BASE_GAP')
                    if msg['fromVersion']<s.book.seq+1:
                        # Snapshot sequence may fall inside event range.
                        s.book.update(msg['bids'],msg['asks'],s.book.seq+1,msg['toVersion'],msg['send_ms'],'mexc')
                    else:
                        s.book.update(msg['bids'],msg['asks'],msg['fromVersion'],msg['toVersion'],msg['send_ms'],'mexc')
                self.mexc_buffer[pair].clear()
            except (KeyError,ValueError,Gap,TypeError) as e:
                s.book.invalid('MEXC_REPLAY_FAILED:'+str(e)[:100])
                self.mexc_buffer[pair].clear()
    def start_mexc(self):
        import websocket
        markets=[s for (v,p),s in self.markets.items() if v=='mexc']
        if not markets:return None
        if len(markets)*2>30:raise ValueError('MEXC_WS_MAX_SUBSCRIPTIONS_EXCEEDED')
        def opened(ws):
            for s in markets:
                self.mexc_buffer[s.pair].clear()
                s.book.invalid('WAIT_FOR_NEW_SESSION_SNAPSHOT');s.tape.connected_at(stamp())
            params=[]
            for s in markets:
                params.extend([f'spot@public.aggre.deals.v3.api.pb@100ms@{s.pair}',
                               f'spot@public.aggre.depth.v3.api.pb@100ms@{s.pair}'])
            ws.send(json.dumps({'method':'SUBSCRIPTION','params':params}))
            def seed():
                time.sleep(2)
                for s in markets:
                    try:
                        snap=http_json('https://api.mexc.com/api/v3/depth',{'symbol':s.pair,'limit':5000})
                        self.seed_mexc(s.pair,snap)
                    except Exception as e:s.book.invalid('MEXC_SEED_FAILED:'+str(e)[:85])
            threading.Thread(target=seed,daemon=True).start()
            def ping():
                while ws.keep_running:
                    try:ws.send(json.dumps({'method':'PING'}))
                    except Exception:return
                    time.sleep(18)
            threading.Thread(target=ping,daemon=True).start()
        def on_data(ws,data,opcode,cont):
            if opcode==websocket.ABNF.OPCODE_BINARY and isinstance(data,bytes):
                self.mexc_message(data)
        def on_message(ws,data):
            if isinstance(data,str):
                try:
                    reply=json.loads(data)
                    if reply.get('code') not in (0,None):
                        self.disconnected('mexc','MEXC_SUB_ERROR:'+str(reply)[:150])
                except (ValueError,TypeError):pass
        def on_error(ws,exc):self.disconnected('mexc','WEBSOCKET_ERROR:'+str(exc)[:90])
        def on_close(ws,status,reason):self.disconnected('mexc','WEBSOCKET_CLOSED:'+str(status))
        app=websocket.WebSocketApp(MEXC,on_open=opened,on_data=on_data,on_message=on_message,
                                   on_error=on_error,on_close=on_close)
        t=threading.Thread(target=lambda:app.run_forever(ping_interval=20,ping_timeout=10,reconnect=5),daemon=True)
        t.start();return t
    def _ws_thread(self,url,venue,markets,handler,on_open):
        import websocket
        def opened(ws):
            for s in markets:
                s.book.invalid('WAIT_FOR_NEW_SESSION_SNAPSHOT')
                s.tape.connected_at(stamp())
            on_open(ws)
        def msg(ws,data):
            if data=='ping':ws.send('pong');return
            try:handler(json.loads(data))
            except (ValueError,TypeError):pass
        def error(ws,exc):
            for s in markets:s.invalidate('WEBSOCKET_ERROR:'+str(exc)[:90])
        def close(ws,status,reason):
            for s in markets:s.invalidate('WEBSOCKET_CLOSED:'+str(status)+':'+str(reason)[:60])
        app=websocket.WebSocketApp(url,on_open=opened,on_message=msg,on_error=error,on_close=close)
        self.ws_clients[venue]=app
        try:app.run_forever(ping_interval=20,ping_timeout=10,reconnect=5)
        finally:
            if self.ws_clients.get(venue) is app:self.ws_clients.pop(venue,None)
    def start_gate(self):
        markets=[s for (v,p),s in self.markets.items() if v=='gate']
        if not markets:return None
        def subscribe(ws):
            for s in markets:
                self.gate_buffer[s.pair].clear()
                ws.send(json.dumps({'time':int(time.time()),'channel':'spot.trades','event':'subscribe','payload':[s.pair]}))
                ws.send(json.dumps({'time':int(time.time()),'channel':'spot.order_book_update','event':'subscribe','payload':[s.pair,'100ms']}))
            def seed():
                time.sleep(2.0) # buffer incremental messages before snapshot
                for s in markets:
                    try:
                        snap=http_json('https://api.gateio.ws/api/v4/spot/order_book',{'currency_pair':s.pair,'limit':100,'with_id':'true'})
                        self.seed_gate_book(s.pair,snap)
                    except Exception as e:s.book.invalid('REST_SEED_FAILED:'+str(e)[:85])
            threading.Thread(target=seed,daemon=True).start()
        t=threading.Thread(target=self._ws_thread,args=(GATE,'gate',markets,self.gate_message,subscribe),daemon=True)
        t.start();return t
    def start_bitget(self):
        markets=[s for (v,p),s in self.markets.items() if v=='bitget']
        if not markets:return None
        def subscribe(ws):
            for s in markets:
                for topic in ('publicTrade','books50'):
                    ws.send(json.dumps({'op':'subscribe','args':[{'instType':'spot','topic':topic,'symbol':s.pair}]}))
        t=threading.Thread(target=self._ws_thread,args=(BITGET,'bitget',markets,self.bitget_message,subscribe),daemon=True)
        t.start();return t
    def recover_unsynced_books(self,interval_ms=60000):
        """Try to restore *live sequence integrity*, never silently bridge an unknown gap.
        Gate/MEXC: new REST snapshot + replay post-snapshot stream increments.
        Bitget: request a fresh channel snapshot through unsubscribe/subscribe.
        Every repair invalidates prior trade continuity; 90s must be observed anew.
        """
        now=stamp()
        for (venue,pair),state in self.markets.items():
            if state.book.synced or now-self.repair_attempts.get((venue,pair),0)<interval_ms:
                continue
            client=self.ws_clients.get(venue)
            if client is None or not getattr(client,'sock',None):continue
            if state.tape.started_ms is not None and now-state.tape.started_ms<45_000:continue
            self.repair_attempts[(venue,pair)]=now
            if venue=='bitget':
                with self.locks[(venue,pair)]:
                    state.invalidate('BITGET_RESUBSCRIBE_PENDING_NEW_SNAPSHOT',now)
                try:
                    arg={'instType':'spot','topic':'books50','symbol':pair}
                    client.send(json.dumps({'op':'unsubscribe','args':[arg]}))
                    client.send(json.dumps({'op':'subscribe','args':[arg]}))
                except Exception as e:
                    state.invalidate('BITGET_REPAIR_SEND_FAILED:'+str(e)[:90],now)
            else:
                def seed(v=venue,p=pair,st=state):
                    try:
                        if v=='gate':
                            snap=http_json('https://api.gateio.ws/api/v4/spot/order_book',
                                {'currency_pair':p,'limit':100,'with_id':'true'})
                            self.seed_gate_book(p,snap)
                        else:
                            snap=http_json('https://api.mexc.com/api/v3/depth',
                                {'symbol':p,'limit':5000})
                            self.seed_mexc(p,snap)
                        if st.book.synced:st.tape.connected_at(stamp())
                    except Exception as e:
                        st.invalidate('REPAIR_FAILED:'+str(e)[:90])
                threading.Thread(target=seed,daemon=True).start()

    def live(self,seconds=1200,checkpoint=None):
        self.start_gate();self.start_bitget();self.start_mexc()
        start=time.monotonic()
        while time.monotonic()-start<seconds:
            if checkpoint:checkpoint(self.report())
            time.sleep(10)
        return self.report()
    def report(self):
        now=stamp()
        reports=[]
        for (venue,pair),s in self.markets.items():
            with self.locks[(venue,pair)]:
                reports.append(s.evaluate(now))
        return {'asof_ms':now,'markets':reports,
                'note':'Public streams only; no promise of uninterrupted 24/7 coverage or future return.'}
