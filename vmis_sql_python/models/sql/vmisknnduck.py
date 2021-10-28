import duckdb
import numpy as np
import pandas as pd

from evaluation.utils import SimpleStopwatch


class VMISSQL:
    def __init__(self):
        self.training_item_ids = None
        self.k = 100
        self.idf_weight = 1
        self.session_id = '-1'
        self.session_items = list()
        self.db = duckdb.connect(database=':memory:', read_only=False)
        self.vsknn = """
            SELECT e_session_id, idf.item_id , (sum_decayed_session_score * idf.idf) as score FROM (
                SELECT e_session_id, item_id, SUM(decayed_session_score) as sum_decayed_session_score FROM (
                    SELECT h_session_id, e_session_id, (MAX(h_session_score) * (1 - (0.1 * MIN(step)))) as decayed_session_score FROM (
                    SELECT h_session_id, h_session_score, e_session_id, e_item_id, step FROM (
                        SELECT h_session_id, h_session_score/CAST(e_distinct_items as DOUBLE) as h_session_score , e_session_id, e_item_id, e_age + 1 as step FROM (
                            SELECT h.session_id as h_session_id, SUM(pos) as h_session_score, evolving_session_20.session_id as e_session_id FROM (
                                SELECT session_id, item_id, MAX(pos) as pos FROM (
                                    SELECT e.session_id, item_id, (session_length-age)/CAST(session_length AS DOUBLE) as pos FROM (
                                       SELECT session_id, COUNT(item_id) as session_length
                                       FROM evolving_sessions 
                                       WHERE session_id = '{session_id}'
                                       GROUP BY session_id
                                    ) as evolving_session_stats -- stats on evolving_session_stats
                                    JOIN evolving_sessions e ON e.session_id = evolving_session_stats.session_id -- evolving session with duplicateitem_ids, pos score for each item_id
                                ) as evolving_session_10 
                                GROUP BY session_id, item_id  -- evolving_session with max pos score per item_id
                            ) as evolving_session_20
                            JOIN historical_sessions h ON h.item_id = evolving_session_20.item_id
                            GROUP BY h.session_id, evolving_session_20.session_id
                            ORDER BY h_session_score DESC
                            LIMIT '{k}'  -- top k h_session_id, h_session_score and e_session_id
                        ) as similar_sessions_10  -- dot product between all history_sessions and evolving_sessions
                        JOIN 
                        (
                            SELECT session_id, item_id as e_item_id, MAX(distinct_items) as e_distinct_items, MIN(age) as e_age FROM (
                            SELECT e.session_id, item_id, session_length, distinct_items, age FROM (
                               SELECT session_id, COUNT(item_id) as session_length, COUNT(DISTINCT(item_id)) as distinct_items
                               FROM evolving_sessions 
                               WHERE session_id = '{session_id}'
                               GROUP BY session_id
                            ) as evolving_session_stats
                            JOIN evolving_sessions e ON e.session_id = evolving_session_stats.session_id
                            ) as evolving_session_10
                            GROUP BY session_id, item_id
                        ) as evolving_session_20
                        ON similar_sessions_10.e_session_id = evolving_session_20.session_id
                    ) as evolving_session_with_h_session_id_and_score  -- h_session_id, h_session_score with all e_item_id for every e_session_id
                    ) as prepare_for_item_scoring
                    JOIN historical_sessions h ON h.session_id = prepare_for_item_scoring.h_session_id AND h.item_id = prepare_for_item_scoring.e_item_id
                    GROUP BY h_session_id, e_session_id
                    HAVING MIN(step) <= 100  -- h_session_id, e_session_id, decayed_session_score
                ) as decayed_session_scores
                JOIN historical_sessions ON historical_sessions.session_id = decayed_session_scores.h_session_id
                GROUP BY e_session_id, item_id
            ) as session_with_recos_10 -- e_session_id, item_id, sum_decayed_session_score
            JOIN idf ON idf.item_id = session_with_recos_10.item_id
            ORDER BY score DESC
        ;"""

        self.prediction_sw = SimpleStopwatch()


    def fit(self, data):
        self.training_item_ids = data['ItemId'].unique()

        data = data.rename(columns={'ItemId': 'item_id', 'SessionId': 'session_id', 'Time': 'time'}, inplace=False)

        self.db.register('data_df', data)
        self.db.execute("""CREATE TABLE historical_sessions AS SELECT session_id, item_id, MAX(CAST(time as INTEGER)) as time 
                   FROM data_df
                   GROUP BY session_id, item_id
                   """)  # we remove revisited products during the same session.
        self.db.unregister('data_df')
        self.db.execute("CREATE TABLE evolving_sessions (session_id VARCHAR, item_id BIGINT, time INTEGER, age INTEGER);")

        qty_distinct_sessions = \
            self.db.execute("select COUNT(DISTINCT(session_id)) as qty_distinct_sessions from historical_sessions") \
                .fetchdf()['qty_distinct_sessions'][0]
        self.db.execute(
            """CREATE TABLE idf AS select item_id, CAST('{idf_weight}' AS INTEGER) * LN(CAST('{qty_distinct_sessions}' AS INTEGER) / CAST(COUNT(item_id) as DOUBLE)) as idf 
                FROM historical_sessions 
                GROUP BY item_id""".format(
                qty_distinct_sessions=qty_distinct_sessions, idf_weight=self.idf_weight))
        self.db.execute(
            """CREATE UNIQUE INDEX idf_item_id_idx ON idf (item_id);""")
        self.db.execute(
            """CREATE INDEX historical_sessions_item_id_idx ON historical_sessions (item_id);""")
        self.db.execute(
            """CREATE INDEX historical_sessions_session_id_idx ON historical_sessions (session_id);""")

    def predict_next(self, session_id, input_item_id, timestamp=0, skip=False, type='view'):

        if self.session_id != session_id:  # new session
            self.session_id = session_id
            self.session_items.clear()
            self.db.execute("DELETE FROM evolving_sessions")
        self.session_items.append(input_item_id)
        self.prediction_sw.start()
        self.db.execute(
            "INSERT INTO evolving_sessions (session_id, item_id, time, age) VALUES('{session_id}', {item_id}, {time}, -1);".format(
                    session_id=session_id, item_id=input_item_id, time=timestamp))
        self.db.execute("UPDATE evolving_sessions SET age = age +1 WHERE session_id = '{session_id}';".format(
            session_id=session_id))

        neighbors = self.db.execute(self.vsknn.format(
                session_id=session_id, k=self.k)).fetchdf()
        self.prediction_sw.stop(len(self.session_items))
        scores = neighbors.set_index('item_id')['score'].to_dict()  # Convert to same format as original VS-KNN code.
        # Create things in the format .. copy-paste from original VS-KNN code.
        predictions = np.zeros(len(self.training_item_ids))
        mask = np.in1d(self.training_item_ids, list(scores.keys()))
        items = self.training_item_ids[mask]
        values = [scores[x] for x in items]
        predictions[mask] = values
        series = pd.Series(data=predictions, index=self.training_item_ids)
        series[input_item_id] = 0
        series = series / series.max()
        series[np.isnan(series)] = 0
        series.sort_values(ascending=False, inplace=True)

        return series

    def get_latencies(self):
        return self.prediction_sw.get_prediction_latencies_in_micros()
