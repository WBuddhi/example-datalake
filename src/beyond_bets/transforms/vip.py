from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from beyond_bets.transforms.market_analyser import MarketAnalyser
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = SparkSession.builder.getOrCreate()


class TopPlayers(Transform):
    def __init__(self):
        super().__init__()
        self._name: str = "TopPlayers"
        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        window_spec = Window.orderBy(F.col("total_bets").desc())
        total_spend_df = MarketAnalyser(
            entity="player_id", time_scope="day"
        ).result()
        total_spend_df = (
            total_spend_df.filter(
                F.col("date") > (datetime.now() - timedelta(days=7))
            )
            .withColumn("percent_rank", F.percent_rank().over(window_spec))
            .filter(F.col("percent_rank") <= 0.1)
        )
        return total_spend_df
