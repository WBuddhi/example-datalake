from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as fn


class InvalidEntityException(Exception):
    def __init__(self):
        super().__init__(
            "Invalid entity: allowed values are either `player_id`, `market` or `all`"
        )


class InvalidTimeScopeException(Exception):
    def __init__(self):
        super().__init__(
            "Invalid time scope: allowed values are either `day` or `hour`"
        )


spark = SparkSession.builder.getOrCreate()


class MarketAnalyser(Transform):
    """
    A class used to analyze betting market data, grouped by entity and time
    scope.

    This class allows you to analyze data related to bets, grouping the data
    by a specified entity (such as `player_id`, `market` or `all`) and a time scope
    (such as `day` or `hour`). It is designed to handle market data and
    transform it based on the specified entity and time scope.

    Args:
        entity (str, optional): The entity to group the data by. Can be either
            "player_id", "market" or "all". Defaults to "player_id".
        time_scope (str, optional): The time scope for grouping the data. Can
            be either "day" or "hour". Defaults to "day".
    """

    def __init__(self, entity: str = "player_id", time_scope: str = "day"):
        super().__init__()
        self.alias = {"day": "date", "hour": "hour"}
        self._set_entity_and_group_by(entity)
        self._set_time_scope(time_scope)

        name_prefix = {"day": "Daily", "hour": "Hourly"}
        self._name = f"{entity.lower().capitalize()}{name_prefix[time_scope]}"
        self._inputs = {"bets": Bets()}

    def _set_entity_and_group_by(self, entity: str):
        allowed_entities = ["player_id", "market"]
        if entity.lower() not in allowed_entities and entity.lower() != "all":
            raise InvalidEntityException
        if entity.lower() == "all":
            self.group_by = allowed_entities
        else:
            self.group_by = [entity.lower()]
        self.entity = entity

    def _set_time_scope(self, time_scope: str):
        if time_scope not in ["day", "hour"]:
            raise InvalidTimeScopeException
        self.time_scope = time_scope
        self.group_by.append(self.alias[time_scope])

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        return (
            self.bets.repartition(100)
            .withColumn(
                self.alias[self.time_scope],
                fn.date_trunc(self.time_scope, fn.col("timestamp")),
            )
            .groupBy(self.group_by)
            .agg(fn.sum(fn.col("bet_amount")).alias("total_bets"))
        )
