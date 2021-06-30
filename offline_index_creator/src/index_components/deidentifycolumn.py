from pyspark import keyword_only, SQLContext
from pyspark.ml import Transformer
import pyspark.sql.functions as fn
from pyspark.sql.window import Window as wn
from pyspark.ml.param.shared import *
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class DeIdentifyColumn(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    """
    This component de-identifies a column by assigning a unique number (index starting with 0) for every unique value in the src_column_name.
    1) This makes it harder to identify the true value in the original column.
    2) The column values are consecutive numbers starting from zero. Thus the values will fit in the smallest uint possible.
    3) The column value can be used as an index in an array
    Use this component to create datasets that can be used for testing purposes without exposing potential personal-identifiable-information.
    Usage:
        my_transformer = DeIdentifyColumn(inputCol='ItemId', outputCol='DeIdItemId')
        df2 = my_transformer.transform(df)
    :return:
        a dataframe with an extra column
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(DeIdentifyColumn, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df):
        print(self.__class__.__name__ + '._transform() called')
        inputCol = self.getInputCol()
        # input and output column could be the same, so we use an intermediate output column
        intermediateOutputCol = self.getOutputCol() + "_" + self.__class__.__name__
        session_id_df = df.select([inputCol]).distinct()
        df_with_seq_id = session_id_df.withColumn(intermediateOutputCol,
                                                  fn.row_number().over(
                                                      wn.orderBy(fn.monotonically_increasing_id())) - 1)
        # work around 'spark cant find input column when joining'
        renamedColumnName = inputCol + '_right'
        df_with_seq_id_10 = df_with_seq_id.withColumnRenamed(inputCol, renamedColumnName)
        df2 = df.join(df_with_seq_id_10, on=df[inputCol] == df_with_seq_id_10[renamedColumnName])
        outputCol = self.getOutputCol()
        result = df2.withColumn(outputCol, fn.col(intermediateOutputCol))
        result = result.drop(*[renamedColumnName, intermediateOutputCol])
        return result