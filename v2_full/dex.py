"""Pancake V3 active-tick ask capacity via live eth_call, NOT pool balance.
Infinity pool IDs, unsupported pool types, partial tick maps and stale RPC fail closed.
No quote conversion to USD is assumed (WBNB is not a dollar).
"""
from __future__ import annotations
from decimal import Decimal, localcontext
import math
from .core import D, dec, stamp

Q96=D(2)**96
FACTORY_PANCAKE_V3_BSC='0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865'
SEL={'slot0':'0x3850c7bd','liquidity':'0x1a686502','tickSpacing':'0xd0c93a7c',
     'tickBitmap':'0x5339c296','ticks':'0xf30dba93','token0':'0x0dfe1681',
     'token1':'0xd21220a7','fee':'0xddca3f43','factory':'0xc45a0155','decimals':'0x313ce567'}

def argint(value):return (int(value)%(2**256)).to_bytes(32,'big').hex()
def signed(value,bits):
    value=int(value);return value-(1<<bits) if value & (1<<(bits-1)) else value

def selector_int(http,url,address,method,*args,block=None):
    data=SEL[method]+''.join(argint(z) for z in args)
    if block is None:raise ValueError('PINNED_FINALIZED_BLOCK_REQUIRED')
    result=http.rpc(url,'eth_call',[{'to':address,'data':data},hex(block)])
    if not isinstance(result,str) or not result.startswith('0x') or len(result)<66:
        raise ValueError('ETH_CALL_FAILED:'+method)
    return int(result[2:66],16),result

def addr_from_word(word):return '0x'+word[-40:].lower()

def tick_sqrt(tick):
    with localcontext() as ctx:
        ctx.prec=70
        return (D('1.0001')**D(tick)).sqrt()

def v3_raw_ask_quote(sqrt_x96,L,ticks,token_is_0,up_pct=D('.02')):
    """Native quote input required for moving token/quote marginal price by up_pct.
    ticks: {tick:int_liquidityNet} for every initialized tick within the interval.
    Fee excludes; liquidity gaps are detected. This is an AMM spot *curve*, not CEX inventory.
    """
    with localcontext() as ctx:
        ctx.prec=70
        s=D(sqrt_x96)/Q96;L=D(L);up=dec(up_pct)
        if s<=0 or L<=0 or up<=0:raise ValueError('INVALID_POOL_STATE')
        target=s*(D(1)+up).sqrt() if token_is_0 else s/(D(1)+up).sqrt()
        direction=1 if token_is_0 else -1
        needed=D(0);current=s
        barriers=sorted([(tick_sqrt(tick),tick,dec(net)) for tick,net in ticks.items()
                       if (direction==1 and s<tick_sqrt(tick)<target)
                       or (direction==-1 and target<tick_sqrt(tick)<s)],reverse=direction==-1)
        barriers.append((target,None,None))
        for next_s,tick,net in barriers:
            if L<=0:raise ValueError('ZERO_ACTIVE_LIQUIDITY')
            if direction==1:
                needed+=L*(next_s-current) # quote token1 required
            else:
                needed+=L*(D(1)/next_s-D(1)/current) # quote token0 required
            current=next_s
            if tick is not None:
                L+=(net if direction==1 else -net)
                if L<0:raise ValueError('NEGATIVE_POST_TICK_LIQUIDITY')
        return needed


def v3_raw_ask_token_quantity(sqrt_x96,L,ticks,token_is_0,up_pct=D('.02')):
    """Verified V3 curve's target-token output for a +up_pct token-price move.

    LiquidityNet is crossed at every initialized tick in the direction of the
    token purchase. No inference about off-chain CEX supply, fees or LP intent.
    """
    with localcontext() as ctx:
        ctx.prec=70
        s=D(sqrt_x96)/Q96;L=D(L);up=dec(up_pct)
        if s<=0 or L<=0 or up<=0:raise ValueError('INVALID_POOL_STATE')
        target=s*(D(1)+up).sqrt() if token_is_0 else s/(D(1)+up).sqrt()
        direction=1 if token_is_0 else -1
        barriers=sorted([(tick_sqrt(tick),tick,dec(net)) for tick,net in ticks.items()
                         if (direction==1 and s<tick_sqrt(tick)<target)
                         or (direction==-1 and target<tick_sqrt(tick)<s)],
                        reverse=direction==-1)
        barriers.append((target,None,None))
        received=D(0);current=s
        for next_s,tick,net in barriers:
            if L<=0:raise ValueError('ZERO_ACTIVE_LIQUIDITY')
            if direction==1:
                received+=L*(D(1)/current-D(1)/next_s)  # token0 out
            else:
                received+=L*(current-next_s)             # token1 out
            current=next_s
            if tick is not None:
                L+=(net if direction==1 else -net)
                if L<0:raise ValueError('NEGATIVE_POST_TICK_LIQUIDITY')
        return received


def probe_v3(http,url,pool,token_address,block,expected_factory=FACTORY_PANCAKE_V3_BSC):
    try:
        factory=addr_from_word(selector_int(http,url,pool,'factory',block=block)[1][2:66])
        t0=addr_from_word(selector_int(http,url,pool,'token0',block=block)[1][2:66])
        t1=addr_from_word(selector_int(http,url,pool,'token1',block=block)[1][2:66])
        if factory!=expected_factory:
            raise ValueError('V3_FACTORY_NOT_MATCHED')
        token=token_address.lower()
        if token not in (t0,t1):raise ValueError('COIN_NOT_IN_POOL_TOKEN0_TOKEN1')
        d0=selector_int(http,url,t0,'decimals',block=block)[0]
        d1=selector_int(http,url,t1,'decimals',block=block)[0]
        if not 0<=d0<=36 or not 0<=d1<=36:raise ValueError('INVALID_TOKEN_DECIMALS')
        x,raw=selector_int(http,url,pool,'slot0',block=block)
        words=[int(raw[2+i*64:2+(i+1)*64],16) for i in range(7)]
        sqrt_x96,tick=words[0],signed(words[1]&((1<<24)-1),24)
        L=selector_int(http,url,pool,'liquidity',block=block)[0]
        spacing=selector_int(http,url,pool,'tickSpacing',block=block)[0]
        if not 1<=spacing<=1000 or not sqrt_x96 or not L:raise ValueError('INVALID_SQRT_L_SPACING')
        if t0==token:
            higher_tick=tick+math.ceil(math.log(1.02)/math.log(1.0001))+spacing*2
            lower_tick=tick
        else:
            lower_tick=tick-math.ceil(math.log(1.02)/math.log(1.0001))-spacing*2
            higher_tick=tick
        start=lower_tick//spacing;end=higher_tick//spacing
        active={}
        words_map={}
        for w in range(start//256,end//256+1):
            bits=selector_int(http,url,pool,'tickBitmap',w,block=block)[0]
            words_map[w]=bits
        for compressed in range(start,end+1):
            word=compressed//256;offset=compressed%256
            if (words_map[word]>>offset)&1:
                ti=compressed*spacing
                liquidity_word=selector_int(http,url,pool,'ticks',ti,block=block)[1]
                # ticks returns liquidityGross (uint128), liquidityNet (int128), ...
                liquiditynet=signed(int(liquidity_word[2+64:2+128],16)&((1<<128)-1),128)
                active[ti]=liquiditynet
        token_is_0=t0==token
        quote=t1 if token_is_0 else t0
        dec_quote=d1 if token_is_0 else d0
        gross=v3_raw_ask_quote(sqrt_x96,L,active,token_is_0)
        token_qty=v3_raw_ask_token_quantity(sqrt_x96,L,active,token_is_0)
        dec_token=d0 if token_is_0 else d1
        result={'status':'V3_ACTIVE_TICK_2PCT_RAW_QUOTE_ESTIMATED',
                'pool':pool,'factory':factory,'token0':t0,'token1':t1,'target':token,
                'quote_token':quote,'quote_token_decimals':dec_quote,'quote_needed_for_2pct_price_impact':str(gross/(D(10)**dec_quote)),
                'token_qty_out_before_2pct_price_impact':str(token_qty/(D(10)**dec_token)),
                'token_qty_source':'PINNED_V3_FULL_INITIALIZED_TICKS_WITHIN_2PCT',
                'onchain_liquidity_raw':str(L),'sqrt_price_x96':str(sqrt_x96),'active_tick':tick,
                'initialized_ticks_in_band':len(active),'scope':'Current pool curve only; does not include other pools/CEX asks, no USD conversion, swap fees excluded; quote capacity variation alone does not prove LP withdrawal',
                'collected_ms':stamp(),'pinned_finalized_block':block}
        return result
    except Exception as e:
        return {'status':'V3_PROBE_FAIL_CLOSED','pool':pool,'error':type(e).__name__+':'+str(e)[:180]}
