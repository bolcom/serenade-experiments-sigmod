import pyspark.sql.functions as fn
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import *
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class MostRecentSessionItem(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    '''
    MostRecentSessionItem:
        During a session a visitor can browse the same item multiple time.
        This transformer removes duplicate item_ids from the same session and only keeps the one max timestamp for that ItemId
        To reduce the size of this dataframe.
        :returns a dataframe with columns
        """
        result
        +----------+----------------+----------+
        | SessionId|          ItemId| Timestamp|
        +----------+----------------+----------+
        |   1799819|        21875077|1612172042|
        |    281624|        99487091|1611082703|
        |    298575|        99487091|1610560559|
        |  10568815|        99487091|1616926796|
        +----------+----------------+----------+
        """

    '''
    @keyword_only
    def __init__(self):
        super(MostRecentSessionItem, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df):
        print(self.__class__.__name__ + '._transform() called')
        df = df.groupBy(['SessionId', 'ItemId']).agg(fn.max(fn.col('Time')).alias('Time'))
        return df
