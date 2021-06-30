import pyspark.sql.functions as fn
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import *
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class SelectCommerciallyViableCustomers(Transformer,DefaultParamsReadable, DefaultParamsWritable):
    """
    This component selects click data from customers from the input_df.
    We only select click data from commercially viable customers.
    :return:
        a selection of rows from the input dataframe that contains commercially viable customers (fewer rows)
    """
    @keyword_only
    def __init__(self):
        super(SelectCommerciallyViableCustomers, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, df):
        print(self.__class__.__name__ + '._transform() called')
        purchases10 = df.filter(fn.col('qty_purchased') > fn.lit(0))
        selected_buis = purchases10.select(['bui']).distinct()
        # customers can click on a product via the ATC button without visiting the PDP page.
        df = df.withColumn('qty_detailpage', fn.col('qty_detailpage') + fn.col('qty_add_to_cart'))
        df = df.filter(fn.col('qty_detailpage') > fn.lit(0))
        df2 = df.join(selected_buis, on=['bui'])
        # only return rows with click events
        return df2
