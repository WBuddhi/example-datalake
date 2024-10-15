from datetime import datetime, timedelta
from pyspark.sql import functions as F
from beyond_bets.transforms.player_daily import PlayerDaily
from beyond_bets.transforms.market_hourly import MarketHourly
from beyond_bets.transforms.market_analyser import MarketAnalyser
from beyond_bets.transforms.vip import TopPlayers
from beyond_bets.transforms.market_conditions import BetGrader


pd = MarketAnalyser(time_scope = "day", entity = "player_id")
pd.result().orderBy(F.col("total_bets").desc()).limit(10).show()

ph = MarketAnalyser(time_scope = "hour", entity = "player_id")
ph.result().orderBy(F.col("total_bets").desc()).limit(10).show()

md = MarketAnalyser(time_scope = "day", entity = "market")
md.result().orderBy(F.col("total_bets").desc()).limit(10).show()

mh = MarketAnalyser(time_scope = "hour", entity = "market")
mh.result().orderBy(F.col("total_bets").desc()).limit(10).show()

pmd = MarketAnalyser(time_scope = "day", entity = "all")
pmd.result().orderBy(F.col("total_bets").desc()).limit(10).show()


pmd = TopPlayers()
pmd.result().orderBy(F.col("total_bets").desc()).limit(10).show()


pmd = BetGrader(start_time = (datetime.now() - timedelta(days=1)))
pmd.result().show()
