from quantopian.pipeline.data import Fundamentals
from quantopian.pipeline.data.builtin import USEquityPricing

from quantopian.algorithm import attach_pipeline, pipeline_output

from quantopian.pipeline import Pipeline, CustomFactor
from quantopian.pipeline.factors import Returns, SimpleMovingAverage, AverageDollarVolume, Latest

from quantopian.pipeline.filters import StaticAssets
from quantopian.pipeline.classifiers.fundamentals import Sector
from quantopian.pipeline.domain import US_EQUITIES

import pandas as pd
import numpy as np

# Called once at the start of the simulation.
def initialize(context):
    attach_pipeline(make_pipeline(), 'Momentum Trading')
    
    context.short_bets = None
    context.long_bets = None
    context.output = None
    
    # Schedule trading function to be called at the start of each trading day
    schedule_function(trade, date_rules.every_day(), time_rules.market_open(hours=0, minutes=1)) 


def make_pipeline():
    # Windows used for moving averages    
    windows = [10,20,50]
    
    price = USEquityPricing.close.latest
    
    # Let's calculate moving averages based on our windows
    price_ma_10 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=windows[0])
    price_ma_20 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=windows[1])
    price_ma_50 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=windows[2])
    
    # Score the divergence
    momentum_score = .67*((price_ma_10 - price_ma_20)/price_ma_20) + .33*((price_ma_20 - price_ma_50)/price_ma_50)
    momentum_abs = momentum_score.abs()
    
    # Top decile of momentum_scores are our longs and bottom ten are shorts
    score_deciles = momentum_score.deciles()
    bottom_ten = score_deciles.eq(0)
    top_ten = score_deciles.eq(9)
    
    ################################################
    # Now let's filter out all stocks we don't want:
    ################################################
    
    # Screening for short positions
    shorts = bottom_ten
    
    # Screening for long positions
    longs = top_ten
    
    # Let's get dollar volume moving averages for each stock
    dv_ma_20 = AverageDollarVolume(window_length=20)
    dv_ma_5 = AverageDollarVolume(window_length=5)
    
    # We only want stocks with notable trade volume >$5,000,000
    dv_min = dv_ma_20 > 5e6
    
    # We want technology sector stocks
    sector = Sector().eq(Sector.TECHNOLOGY)
    
    # We don't want super cheap stocks
    not_cheap_stock = (price_ma_10 > 5)
    
    # Check for increasing trade volume
    inc_volume = dv_ma_5 > dv_ma_20
    
    # Universe of stocks we want to trade that fit all our criteria
    universe = sector & not_cheap_stock & dv_min & inc_volume & (shorts | longs)
    
    return Pipeline(
        columns={
            'price': price,
            'price_ma_10': price_ma_10,
            'price_ma_20': price_ma_20,
            'price_ma_50': price_ma_50,
            'dollar_volume': dv_ma_5,
            'momentum_score': momentum_score,
            'momentum_abs': momentum_abs,
            'long_bets': longs,
            'short_bets': shorts,
        },
        screen=universe, 
        domain=US_EQUITIES,
    )

def handle_data(context, data):
    pass

def before_trading_start(context, data):
    # Retrieve pipeline output and store in context
    context.output = pipeline_output('Momentum Trading') #.dropna(axis=0)
    
    # Store bets
    context.long_bets = set(context.output[context.output['long_bets']].index.tolist())
    context.short_bets = set(context.output[context.output['short_bets']].index.tolist())
    context.bets = list(context.long_bets | context.short_bets)
    
    # Calculate gross momentum of all bets we want to make
    context.total_momentum = np.sum(np.absolute(context.output['momentum_score']))    
    
   
# Called at start of each trading day
def trade(context, data):
    # Sell out of positions we have no views on
    for stock in context.portfolio.positions:
        if not stock in context.bets:
            order_target_percent(stock, 0)
    
    # Bet on shorts/longs weighted by momentum_score
    for stock in context.bets:
        weight = context.output.loc[stock].loc['momentum_score'] / context.total_momentum
        order_target_percent(stock, weight)