import argparse
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as fn
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel, Pipeline
from collections import namedtuple
from pyspark.sql.types import (ArrayType, DoubleType, StructType, LongType, IntegerType, LongType,
                               StructField, StringType)

from src.index_components.most_recent_session_item_pair import MostRecentSessionItem
from src.index_components.select_commercially_active_visitors import SelectCommerciallyViableCustomers
from src.index_components.min_column_support import MinimumColumnSupport
from src.index_components.sessionizer import Sessionizer
from src.index_components.deidentifycolumn import DeIdentifyColumn


def create_serenade_indexes(catalog_input_dir, qty_lookback_days, end_date, base_input_dir, base_output_dir):
    if not end_date:
        print('error: {} is an invalid formatted date. use something like: 2020-06-22'.format(end_date))

    # Configuration settings
    app_settings = {'qty_lookback_days': int(qty_lookback_days),
                    'min_item_support': 5,
                    'min_session_length' : 2,
                    'qty_most_recent_sessions_per_item' : 500,
                    'end_date': end_date
                    }

    for key in sorted(app_settings.keys()):
        print("{}: {}".format(key, app_settings[key]))
    params = namedtuple('app_settings', app_settings.keys())(
        **app_settings)

    spark = (SparkSession.builder
             .getOrCreate())
    sc = spark.sparkContext
    sql_context = SQLContext(sc)
    sql_context.setConf("spark.sql.shuffle.partitions", "1200")
    sql_context.setConf("spark.sql.avro.compression.codec", "snappy")

    def previous_days(current_date, qty_lookback_days):
        from datetime import datetime, timedelta
        result = []
        previous_day = datetime.strptime(current_date, "%Y-%m-%d")
        for i in range(qty_lookback_days):
            result.append(previous_day.strftime("%Y-%m-%d"))
            previous_day = previous_day - timedelta(days=1)
        return result

    prev_days = previous_days(params.end_date, params.qty_lookback_days)
    context_path = base_input_dir + '/{' + ','.join(prev_days) + '}'
    print('context_path:' + str(context_path))

    events10 = sql_context.read.format("avro").load(context_path)
    events10 = events10.filter((fn.col('qty_detailpage') + fn.col('qty_purchased')) > fn.lit(0))
    events10 = events10.withColumnRenamed('item_id', 'ItemId')
    events10 = events10.withColumn('ItemId', fn.col('ItemId').cast(LongType()))
    events10 = events10.withColumn('Time', (fn.col('timestamp') / fn.lit(1000)).cast(IntegerType()))  # milliseconds to seconds conversion
    events10 = events10.drop(*['timestamp'])

    my_pipeline = Pipeline(stages=[SelectCommerciallyViableCustomers(),
                                    Sessionizer(),
                                    MostRecentSessionItem(),
                                    # minimum support for session, item, session.
                                    MinimumColumnSupport(inputCol='SessionId', min_support=params.min_session_length),
                                    MinimumColumnSupport(inputCol='ItemId', min_support=params.min_item_support),
                                    MinimumColumnSupport(inputCol='SessionId', min_support=params.min_session_length),
                                   ])
    full_df = my_pipeline.fit(events10).transform(events10)
    # Rename SessionId to VisitId to have more distinctive column names when exporting the sessionindex.
    full_df = full_df.withColumnRenamed('SessionId', 'VisitId')

    # Replace Time when clicked on an ItemId with the Max timestamp of the visit.
    df_visit_statis = full_df.groupBy(['VisitId']).agg(
        fn.max(fn.col('Time')).alias('Time'),
        fn.count(fn.col('ItemId')).alias('qty_items')
    )
    p995 = df_visit_statis.agg(fn.expr('percentile(qty_items, array(0.995))')[0].alias('p995')).collect()[0][0]
    print('removing training sessions of length > p99.5 :', str(p995))
    df_visit_statis = df_visit_statis.filter(fn.col('qty_items') <= fn.lit(p995))
    full_df = full_df.drop(*['Time'])
    full_df = full_df.join(df_visit_statis, on=['VisitId'])

    # we now extract the four indexes.
    # start index3: item_id to session_idf score on the _raw_ population data
    qty_unique_session_ids = full_df.select(['VisitId']).distinct().count()
    print("qty_unique_session_ids:" + str(qty_unique_session_ids))
    itemid_to_idf_df = full_df.groupBy(['ItemId']).agg(fn.log((qty_unique_session_ids / fn.count(fn.col('VisitId')))).alias('idf'))
    # end index3: item_id to session_idf score on the _raw_ population data

    # start index1: item to top-m most recent session_ids (but with the original session_ids)
    window1 = Window.partitionBy(['ItemId']).orderBy(fn.col('Time').desc())
    rank_desc1 = fn.row_number().over(window1).alias('timestamp_rank_desc')
    itemid_with_top_visits = full_df.select('*', rank_desc1).filter(
        fn.col('timestamp_rank_desc') <= params.qty_most_recent_sessions_per_item)
    #  Row(VisitId='123', ItemId=1001004,Time=1618518249, timestamp_rank_desc=1),
    #  Row(VisitId='123', ItemId=1001002,Time=1618514789, timestamp_rank_desc=2),
    #  Row(VisitId='123', ItemId=1001012,Time=1618407122, timestamp_rank_desc=3),
    #  Row(VisitId='456', ItemId=4004798,Time=1618504822, timestamp_rank_desc=1),
    #  Row(VisitId='789', ItemId=1098406,Time=1618500012, timestamp_rank_desc=2)]
    # end index1: item to top-m most recent session_ids (but with the original session_ids)

    # we now add a (vector position) index to the distinct SessionId values
    top_visit_ids = itemid_with_top_visits.select(['VisitId']).distinct()
    my_indexer = DeIdentifyColumn(inputCol='VisitId', outputCol='SessionIndex')
    top_visit_ids_with_indices = my_indexer.transform(top_visit_ids)
    # [Row(VisitId='123', SessionIndex=3125),
    #  Row(VisitId='456', SessionIndex=1993),
    #  Row(VisitId='789', SessionIndex=2916),
    #  Row(VisitId='8484', SessionIndex=2052),
    #  Row(VisitId='89462936', SessionIndex=621)]
    # top_visit_ids_with_indices.count() => 651121
    # top_visit_ids_with_indices.select(['VisitId']).distinct().count() => 651121
    # top_visit_ids_with_indices.select(['SessionIndex']).distinct().count() => 651121

    # start index2 and index4: session_id to unique item_ids and maxtimstamp. Item ids are sorted numerically in ascending order.
    candidate_sessions_df = full_df.join(top_visit_ids_with_indices, on=['VisitId'])
    candidate_sessions_df.select(['SessionIndex', 'ItemId', 'Time']).repartition(100).write.mode("overwrite").parquet(base_output_dir + "/tmp/candidate_sessions_df")
    candidate_sessions_df = sql_context.read.parquet(base_output_dir + "/tmp/candidate_sessions_df")

    sessionindex_to_items_and_time_df = candidate_sessions_df.groupBy(['SessionIndex']).agg(
        fn.sort_array(fn.collect_list(fn.col('ItemId'))).alias('item_ids_asc'),
        fn.max(fn.col('Time')).alias('Time')
    )
    # end index2 and index4: session_id to unique item_ids and maxtimstamp. Item ids are sorted numerically in ascending order.

    # DataFrame[VisitId: string, ItemId: bigint, Time: int, timestamp_rank_desc: int, SessionIndex: int]
    # we sort to garantuee the correct sequence of sessions.
    window2 = Window.partitionBy(['ItemId']).orderBy(fn.col('Time').desc())
    rank_desc2 = fn.row_number().over(window2).alias('timestamp_rank_desc')
    itemid_with_top_visits = candidate_sessions_df.select('*', rank_desc2).filter(
        fn.col('timestamp_rank_desc') <= params.qty_most_recent_sessions_per_item)
    item_id_to_most_recent_sessionindices_df = itemid_with_top_visits.groupBy(['ItemId']).agg(fn.collect_list(fn.col('SessionIndex')).alias('session_indices_time_ordered'))
    # end use vector_index as session_id for index1. This must be done after we know which session_ids where indexed.

    print('training sessions of length <= p99.5 :', str(p995))
    item_index = item_id_to_most_recent_sessionindices_df.join(itemid_to_idf_df, on=['ItemId'])

    catalog = sql_context.read.format("avro").load(catalog_input_dir)
    # Rename globalId to ItemId so we can easily join later on
    catalog = catalog.withColumnRenamed('globalId', 'ItemId')
    # Greatly reduce the catalog size by filtering only the item_ids in the final output
    unique_item_ids = item_index.select(['ItemId'])
    catalog = catalog.join(unique_item_ids, on=['ItemId'])
    pron_df10 = catalog.select(fn.col('ItemId'), fn.explode(fn.col('attributes')).alias('attr'))
    pron_df10 = pron_df10.filter(fn.col('attr.name').isin(['Erotic Content']))
    pron_df20 = pron_df10.select(fn.col('ItemId'), fn.explode(fn.col('attr.values')).alias('erotic_content'))
    pron_df30 = pron_df20.filter(fn.lower(fn.col("erotic_content.value")) == fn.lit('yes'))
    pron_df30 = pron_df30.select(['ItemId']).distinct()  # Prevent duplicate globalIds in case a product contains the 'Erotic Content' attribute multiple times.
    pron_df30 = pron_df30.withColumn('IsAdult', fn.lit(True))
    pron_df40 = pron_df30.select(['ItemId', 'IsAdult'])

    for_sale_df = catalog.filter(fn.col('ForSale') == fn.lit(True))
    for_sale_df = for_sale_df.select(['ItemId', 'ForSale'])

    # outer join all product flags such as forsale can isadult
    item_info_df = pron_df40.join(for_sale_df, on=['ItemId'], how='outer')
    # [Row(ItemId='9200000023', ForSale=True, IsAdult=True),
    # Row(ItemId='9200000024', ForSale=null, IsAdult=False),
    # ]

    item_index_joined = item_index.join(item_info_df, on=['ItemId'], how='left')
    item_index_joined = item_index_joined.na.fill(value=False, subset=['ForSale', 'IsAdult'])

    # write files. The ForSale and IsAdult fields can be null.
    item_index_joined.select(['ItemId', 'session_indices_time_ordered', 'idf', 'ForSale', 'IsAdult']).repartition(25).write.mode("overwrite").format("avro").save(base_output_dir + "/avro/itemindex")
    sessionindex_to_items_and_time_df.select(['SessionIndex', 'item_ids_asc', 'Time']).repartition(25).write.mode("overwrite").format("avro").save(base_output_dir + "/avro/sessionindex")

    # start: we also extract smaller indices from the production data for fast development
    # this global_id is also defined in the kubernetes readiness config for the rolling deployment
    harry_potter_global_id = 7464978643
    item_index = sql_context.read.format("avro").load(base_output_dir + "/avro/itemindex")
    # get all related sessions for harry potter
    harry_potter_item_index = item_index.filter(fn.col('ItemId') == fn.lit(harry_potter_global_id))
    hp_session_ids = harry_potter_item_index.withColumn('SessionIndex', fn.explode(
        fn.col('session_indices_time_ordered'))).select(['SessionIndex'])

    session_index = sql_context.read.format("avro").load(base_output_dir + "/avro/sessionindex")

    reduced_session_index = session_index.join(hp_session_ids, on=['SessionIndex'])
    # get all item_ids for the sessions related to harry potter
    hp_item_ids = reduced_session_index.withColumn('ItemId', fn.explode(fn.col('item_ids_asc'))).select(
        ['ItemId']).distinct()
    reduced_item_index = item_index.join(hp_item_ids, on=['ItemId'])
    reduced_item_index_10 = reduced_item_index.withColumn('SessionIndex',
                                                          fn.explode(fn.col('session_indices_time_ordered'))
                                                          ).drop(*['session_indices_time_ordered']).distinct()
    # remove session_ids that are not in the selected_session_ids
    reduced_item_index_20 = reduced_item_index_10.join(hp_session_ids, on=['SessionIndex'])
    reduced_item_index_30 = reduced_item_index_20.groupBy(['ItemId']).agg(
        fn.max(fn.col('idf')).alias('idf'),
        fn.max(fn.col('ForSale')).alias('ForSale'),
        fn.max(fn.col('IsAdult')).alias('IsAdult'),
        # 'session_indices_time_ordered' might no longer be ordered but this is ok for this toy dataset.
        fn.collect_list(fn.col('SessionIndex')).alias('session_indices_time_ordered'),
    )

    # make sure the session index has only the
    reduced_session_index_10 = reduced_session_index.withColumn('ItemId', fn.explode(fn.col('item_ids_asc'))).drop(
        *['item_ids_asc'])
    items_for_sessions = reduced_item_index_30.select(['ItemId']).distinct()
    reduced_session_index_20 = reduced_session_index_10.join(items_for_sessions, on=['ItemId'])
    reduced_session_index_30 = reduced_session_index_20.groupBy(['SessionIndex']).agg(
        fn.max(fn.col('Time')).alias('Time'),
        fn.sort_array(fn.collect_list(fn.col('ItemId'))).alias('item_ids_asc'),
    )

    reduced_item_index_30.select(['ItemId', 'session_indices_time_ordered', 'idf', 'ForSale', 'IsAdult']).repartition(
        2).write.mode("overwrite").format("avro").save(base_output_dir + "/sampled-avro/itemindex")
    reduced_session_index_30.select(['SessionIndex', 'item_ids_asc', 'Time']).repartition(2).write.mode(
        "overwrite").format("avro").save(base_output_dir + "/sampled-avro/sessionindex")
    # end: we also extract smaller indices from the production data for fast development

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog_input_dir")
    parser.add_argument("qty_lookback_days")
    parser.add_argument("end_date")
    parser.add_argument("base_input_dir")   # base_input_dir='gs://my-google-cloud-data/fct/clicks/'
    parser.add_argument("base_output_dir")
    args = parser.parse_args()
    print(args)
    create_serenade_indexes(**vars(args))
