"""Radar V2 unified evidence core. No transactions, no invented predictive claims.

Order books are reconstructible only with consecutive exchange-assigned sequence IDs.
CEX ask depth = visible asks at a *fixed reference price*, not total transferable supply.
"""
from __future__ import annotations
from collections import deque, defaultdict
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field
import time

COINS = ('ON','DOS','HANA','COLLECT','LIGHT','APR','IN','TRUST','B2','CYS','STBL','BSB','AIO','STAR','LYN')
D=Decimal

def dec(x):
    try:
        y=D(str(x))
        if not y.is_finite(): raise ValueError('nonfinite')
        return y
    except (InvalidOperation,ValueError,TypeError) as ex:
        raise ValueError('invalid decimal') from ex

def stamp(): return int(time.time()*1000)

class Gap(Exception): pass

@dataclass
class L2:
    bids:dict=field(default_factory=dict)
    asks:dict=field(default_factory=dict)
    seq:int|None=None
    synced:bool=False
    failure:str|None=None
    last_ms:int|None=None
    start_ms:int|None=None
    history:deque=field(default_factory=lambda:deque(maxlen=2500))
    def invalid(self,reason):
        self.synced=False; self.failure=reason; self.history.clear()
    @staticmethod
    def levels(rows):
        out={}
        for price,quantity,*_ in rows:
            p,q=dec(price),dec(quantity)
            if p<=0 or q<0:raise ValueError('negative price or quantity')
            if q:out[p]=q
        return out
    def _assert_valid(self):
        if not self.bids or not self.asks or max(self.bids)>=min(self.asks):
            self.invalid('EMPTY_OR_CROSSED_BOOK')
            raise Gap('EMPTY_OR_CROSSED_BOOK')
    def snapshot(self,bids,asks,seq,ms):
        self.bids=self.levels(bids); self.asks=self.levels(asks)
        self.seq=int(seq);self.synced=True;self.failure=None
        self.start_ms=self.last_ms=int(ms);self.history.clear()
        self._assert_valid();self.record(ms)
    def update(self,bids,asks,first,last,ms,mode):
        if not self.synced:raise Gap('UNSYNCED')
        if mode=='gate':
            if last<=self.seq:return False
            if first>self.seq+1:
                self.invalid('GATE_SEQUENCE_GAP');raise Gap('GATE_SEQUENCE_GAP')
            if not first<=self.seq+1<=last:
                self.invalid('GATE_SEQUENCE_MISMATCH');raise Gap('GATE_SEQUENCE_MISMATCH')
        elif mode=='mexc':
            if first!=self.seq+1 or last<first:
                self.invalid('MEXC_FROM_TO_VERSION_GAP');raise Gap('MEXC_FROM_TO_VERSION_GAP')
        elif mode=='bitget':
            if first != self.seq or last<=self.seq:
                self.invalid('BITGET_PSEQ_GAP');raise Gap('BITGET_PSEQ_GAP')
        else:raise ValueError('unsupported mode')
        for side,rows in ((self.bids,bids),(self.asks,asks)):
            for p,q,*_ in rows:
                p,q=dec(p),dec(q)
                if p<=0 or q<0:raise ValueError('invalid delta')
                if q==0:side.pop(p,None)
                else:side[p]=q
        self.seq=int(last);self.last_ms=int(ms)
        self._assert_valid();self.record(ms)
        return True
    def record(self,ms):
        # High-frequency book deltas can exceed 10k/5min; sample the reconstructed
        # SEQUENCED state every 5 s so the history spans tens of minutes, not seconds.
        if self.history and int(ms)-self.history[-1][0]<5000:return
        self.history.append((int(ms),self.mid(),self.copy_asks()))
    def mid(self):return (max(self.bids)+min(self.asks))/2
    def copy_asks(self):return tuple(sorted(self.asks.items()))
    @staticmethod
    def depth(asks,reference,bps=200):
        cap=dec(reference)*(1+D(bps)/10000)
        return sum((p*q for p,q in asks if p<=cap),D(0))
    @staticmethod
    def is_covered(asks,reference,bps=200):
        # If outermost observed level is below cap, ask depth MAY be truncated.
        return bool(asks and asks[-1][0]>=dec(reference)*(1+D(bps)/10000))
    @staticmethod
    def cost_for_size(asks,token_qty):
        qty=dec(token_qty);cost=D(0)
        for p,n in asks:
            fill=min(qty,n);cost+=fill*p;qty-=fill
            if qty<=0:return cost
        return None
    def compare(self,now_ms,window_ms=300_000):
        if not self.synced or not self.history:return {'status':'BOOK_NOT_SYNCHRONIZED'}
        if now_ms-self.history[-1][0]>15_000:return {'status':'BOOK_STALE'}
        prior=[z for z in self.history if z[0]<=now_ms-window_ms]
        if not prior:return {'status':'INSUFFICIENT_BOOK_HISTORY'}
        before=prior[-1];after=self.history[-1]
        reference=before[1]
        if not self.is_covered(before[2],reference) or not self.is_covered(after[2],reference):
            return {'status':'TRUNCATED_2PCT_ASK_RANGE'}
        old,new=self.depth(before[2],reference),self.depth(after[2],reference)
        if old<=0:return {'status':'ZERO_REFERENCE_DEPTH'}
        # compare anchored price band to avoid 'depth vanished' illusion after price rises
        return {'status':'VERIFIED_SEQUENCED_BOOK_INTERVAL','from_ms':before[0],'to_ms':after[0],
                'reference_price':str(reference),'ask_before_quote':str(old),
                'ask_after_quote':str(new),'fraction_depleted':str((old-new)/old),
                'visible_slippage_cost_at_before':str(self.cost_for_size(before[2],D('100'))) if self.cost_for_size(before[2],D('100')) is not None else None}

@dataclass
class Tape:
    recent:deque=field(default_factory=lambda:deque(maxlen=200000))
    seen_ids:dict=field(default_factory=dict)
    started_ms:int|None=None
    connected:bool=False
    gap_since:int|None=None
    error:str|None=None
    latest_event_ms:int|None=None
    last_numeric_id:int|None=None
    def connected_at(self,ms):
        self.started_ms=int(ms);self.connected=True;self.gap_since=None;self.error=None
        self.recent.clear();self.seen_ids.clear();self.last_numeric_id=None
        self.latest_event_ms=None
    def disconnected(self,ms,why):
        self.connected=False;self.gap_since=int(ms);self.error=why
    def add(self,tr_id,ms,side,price,quantity,strict_increment=False):
        if not self.connected:return
        ms=int(ms);ident=str(tr_id)
        if side not in ('buy','sell'):raise ValueError('invalid taker side')
        notional=dec(price)*dec(quantity)
        if notional<=0:raise ValueError('invalid trade notional')
        data=(ms,side,notional)
        if ident in self.seen_ids:
            if self.seen_ids[ident] != data:
                self.disconnected(ms,'CONFLICTING_DUPLICATE_TRADE_ID')
                raise Gap('CONFLICTING_DUPLICATE_TRADE_ID')
            return
        if self.latest_event_ms is not None and ms+5000<self.latest_event_ms:
            self.disconnected(ms,'TRADE_TIME_ROLLBACK');raise Gap('TRADE_TIME_ROLLBACK')
        if strict_increment:
            num=int(ident)
            if self.last_numeric_id is not None and num!=self.last_numeric_id+1:
                self.disconnected(ms,'EXCHANGE_TRADE_ID_GAP');raise Gap('EXCHANGE_TRADE_ID_GAP')
            self.last_numeric_id=num
        self.seen_ids[ident]=data;self.recent.append(data)
        self.latest_event_ms=ms if self.latest_event_ms is None else max(self.latest_event_ms,ms)
        # Rebuild a bounded dedupe window; an ID repeated outside this window
        # cannot be treated as an audited contiguous exchange trade stream.
        if len(self.seen_ids)>200000:
            self.seen_ids=dict(list(self.seen_ids.items())[-100000:])
    def window(self,now_ms,window_ms=300_000):
        if not self.connected or self.started_ms is None or self.started_ms>now_ms-window_ms:
            return {'status':'TAPE_WINDOW_NOT_CONTINUOUS'}
        if self.gap_since is not None:return {'status':'TRADE_STREAM_GAP'}
        if self.latest_event_ms is None or now_ms-self.latest_event_ms>30_000:
            return {'status':'TRADE_STREAM_STALE_OR_QUIET','note':'Quiet market indistinguishable from missing heartbeats without transport check'}
        events=[r for r in self.recent if now_ms-window_ms<=r[0]<=now_ms]
        buy=sum((r[2] for r in events if r[1]=='buy'),D(0))
        sell=sum((r[2] for r in events if r[1]=='sell'),D(0))
        return {'status':'LIVE_TRANSPORT_CONTINUOUS_NOT_EXCHANGE_AUDITED',
            'buy_quote':str(buy),'sell_quote':str(sell),'quote_total':str(buy+sell),
            'buy_fraction':str(buy/(buy+sell)) if buy+sell else None,'trades':len(events),
            'window_start_ms':now_ms-window_ms,'window_end_ms':now_ms,
            'strict_sequence_supported':self.last_numeric_id is not None}

@dataclass
class VenueState:
    coin:str
    venue:str
    pair:str
    identity:str='UNVERIFIED_SYMBOL_MATCH'
    book:L2=field(default_factory=L2)
    tape:Tape=field(default_factory=Tape)
    session_started:int|None=None
    errors:list=field(default_factory=list)
    def invalidate(self,why,when=None):
        self.book.invalid(why);self.tape.disconnected(stamp() if when is None else when,why)
        self.errors.append(why)
    def evaluate(self,now):
        # Two independent time scales: early detection must not wait for a slow
        # five-minute candle; 5m is used for stronger confirmation.
        layers={}
        for key,window in (('early_90s',90_000),('confirmed_5m',300_000)):
            b=self.book.compare(now,window_ms=window)
            t=self.tape.window(now,window_ms=window)
            reasons=[]
            if self.identity not in ('VERIFIED_SAME_CONTRACT_NETWORK','VERIFIED_OFFICIAL_HISTORICAL_MEXC_SPOT_LISTING_WITH_ACTIVE_PAIR'):reasons.append('SYMBOL_TO_CONTRACT_IDENTITY_UNVERIFIED')
            if b['status']!='VERIFIED_SEQUENCED_BOOK_INTERVAL':reasons.append(b['status'])
            if t['status']!='LIVE_TRANSPORT_CONTINUOUS_NOT_EXCHANGE_AUDITED':reasons.append(t['status'])
            buy=dec(t['buy_quote']) if t.get('buy_quote') is not None else D(0)
            sell=dec(t['sell_quote']) if t.get('sell_quote') is not None else D(0)
            frac=buy/(buy+sell) if buy+sell else D(0)
            depletion=dec(b['fraction_depleted']) if b.get('fraction_depleted') is not None else D(0)
            olddepth=dec(b['ask_before_quote']) if b.get('ask_before_quote') is not None else D(0)
            min_total=D(150) if key=='early_90s' else D(600)
            threshold=max(D(80),olddepth*D('0.10')) if key=='early_90s' else max(D(300),olddepth*D('0.15'))
            if buy+sell<min_total or buy-sell<threshold or frac<D('0.65'):
                reasons.append('NO_MATERIAL_BUY_IMBALANCE_RELATIVE_TO_VISIBLE_ASK')
            if depletion<(D('0.15') if key=='early_90s' else D('0.20')):
                reasons.append('NO_MEASURED_ASK_DEPLETION')
            late=False
            if b.get('reference_price') is not None and self.book.synced:
                try:
                    move=self.book.mid()/dec(b['reference_price'])-1
                    late=move>D('.15')
                except (ValueError,ZeroDivisionError):pass
            if late:reasons.append('PRICE_ALREADY_MORE_THAN_15PCT_FROM_BASELINE')
            layers[key]={'ready':not reasons,'candidate':len(reasons)==1 and reasons[0]=='SYMBOL_TO_CONTRACT_IDENTITY_UNVERIFIED',
                         'blockers':reasons,'book':b,'tape':t,
                         'late_vs_window':late}
        main=layers['confirmed_5m']
        return {'coin':self.coin,'venue':self.venue,'pair':self.pair,
                'evidence_ready':any(x['ready'] for x in layers.values()),
                'candidate':any(x['candidate'] for x in layers.values()),
                'early_ready':layers['early_90s']['ready'],
                'confirmed_5m_ready':main['ready'],
                'blockers':main['blockers'],'book':main['book'],'tape':main['tape'],
                'layers':layers,
                'note':'Observed aggressive buying and visible ask depletion are NOT a calibrated 10x predictor.'}
