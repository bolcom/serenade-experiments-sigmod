import pyspark.sql.functions as fn
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import *
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.window import Window as wn


class Sessionizer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    '''
    Sessionizer:
        Add a new column 'SessionId' to input_df.
        A unique session_id is determined for events for a given bui within xxx minutes.
        expects the following columns to be present in input_df:
            column: 'bui' Browser Unique Identifier
            column: timestamp time or the sequence of events represented. Greater value means more recent.
        :returns the input_df with an added 'session_id' column. The amount of records stay the same.
    '''
    @keyword_only
    def __init__(self):
        super(Sessionizer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df):
        print(self.__class__.__name__ + '._transform() called')
        MAX_SESSION_IDLE_DURATION_SEC = 20 * 60  # xx mins in seconds
        w = (wn
             .partitionBy(fn.col('bui'))
             .orderBy(fn.col('Time'))
             )

        df2 = df.withColumn('PreviousTime', fn.lag(fn.col('Time')).over(w))
        df2 = df2.withColumn('IsNewSession',
                             fn.when(fn.col('Time') - fn.col('PreviousTime') < fn.lit(
                                 MAX_SESSION_IDLE_DURATION_SEC)
                                     , fn.lit(0)).otherwise(fn.lit(1)))
        df2 = df2.withColumn('SessionId', fn.concat(fn.col('bui'), fn.lit('_'), fn.sum('IsNewSession').over(w)))
        df2 = df2.drop(*['IsNewSession', 'PreviousTime'])
        return df2
