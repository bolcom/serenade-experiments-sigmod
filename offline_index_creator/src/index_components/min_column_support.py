from pyspark import keyword_only
from pyspark.ml import Transformer
import pyspark.sql.functions as fn
from pyspark.ml.param.shared import *
from pyspark.ml.param.shared import HasInputCol, Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class MinimumSupport(Params):
    min_support = Param(Params._dummy(), "min_support", "min_support", typeConverter=TypeConverters.toInt)

    def __init__(self):
        super(MinimumSupport, self).__init__()

    def setMinimumSupport(self, value):
        return self._set(min_support=value)

    def getMinimumSupport(self):
        return self.getOrDefault(self.min_support)

class MinimumColumnSupport(Transformer, HasInputCol, MinimumSupport, DefaultParamsReadable,
                           DefaultParamsWritable):
    '''
    MinimumColumnSupport:
        Only keep rows in the dataframe where the input passes the minimal support.
        The schema from the returned dataframe is the same as the input.
        The amount of returned rows is the same of lesser rows
    '''

    @keyword_only
    def __init__(self, inputCol=None, min_support=1):
        super(MinimumColumnSupport, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, min_support=1):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df):
        print(self.__class__.__name__ + '._transform() called')
        min_support = self.getMinimumSupport()
        inputCol = self.getInputCol()
        print('column: ' + inputCol + ' min_support:' + str(min_support))
        items10 = df.groupBy([inputCol]).agg(fn.count(inputCol).alias('cnt'))
        items10 = items10.filter(fn.col('cnt') >= fn.lit(min_support))
        # pyspark couldnt handle the normal join on=['joincolumn']
        # 'joincolumn' not in dataframe error.
        # worked around this by renaming the rightside join key and removing it later on.
        items20 = items10.select([inputCol]).withColumnRenamed(inputCol, inputCol + '_right')
        df2 = df.join(items20, on=df[inputCol] == items20[inputCol + '_right'])
        df2 = df2.drop(inputCol + '_right')
        return df2

