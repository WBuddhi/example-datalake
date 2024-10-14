from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()


class BetGrader(Transform):
    def __init__(self, start_time: datetime):
        super().__init__()
        self.start_time = start_time
        self._name: str = "TopPlayers"
        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        select_cols = [
            "market",
            "player_id",
            "bet_amount",
            "grade",
            "timestamp",
        ]
        select_cols = [F.col(column) for column in select_cols]

        df = (
            self.bets.repartition(100)
            .withColumn("minute", F.date_trunc("minute", F.col("timestamp")))
            .orderBy(F.col("minute").desc())
            .filter(
                F.col("minute") >= (self.start_time - timedelta(minutes=15))
            )
        )
        df_avg = df.groupBy("market").agg(
            F.avg(F.col("bet_amount")).alias("avg_bet")
        )
        return (
            df.join(df_avg, "market")
            .withColumn(
                "grade", F.try_divide(F.col("bet_amount"), F.col("avg_bet"))
            )
            .select(*select_cols)
        )
