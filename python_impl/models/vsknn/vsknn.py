import time
from _operator import itemgetter

import numpy as np
import pandas as pd

from evaluation.utils import SimpleStopwatch

# source: https://github.com/rn5l/session-rec
# https://github.com/rn5l/session-rec/blob/master/algorithms/knn/vsknn.py
class VMContextKNN:
    def __init__(self, k, sample_size=1000, sampling='recent', similarity='cosine', weighting='div',
                 dwelling_time=False, last_n_days=None, last_n_clicks=None, remind=True, push_reminders=False,
                 add_reminders=False, extend=False, weighting_score='div', weighting_time=False, normalize=True,
                 idf_weighting=False, idf_weighting_session=False, session_key='SessionId', item_key='ItemId',
                 time_key='Time'):

        self.training_item_ids = None
        self.k = k
        self.sample_size = sample_size
        self.sampling = sampling
        self.weighting = weighting
        self.dwelling_time = dwelling_time
        self.weighting_score = weighting_score
        self.weighting_time = weighting_time
        self.similarity = similarity
        self.session_key = session_key
        self.item_key = item_key
        self.time_key = time_key
        self.extend = extend
        self.remind = remind
        self.push_reminders = push_reminders
        self.add_reminders = add_reminders
        self.idf_weighting = idf_weighting
        self.idf_weighting_session = idf_weighting_session
        self.normalize = normalize
        self.last_n_days = last_n_days
        self.last_n_clicks = last_n_clicks

        # updated while recommending
        self.session = -1
        self.session_items = []
        self.relevant_sessions = set()

        # cache relations once at startup
        self.session_item_map = dict()
        self.item_session_map = dict()
        self.session_time = dict()
        self.min_time = -1

        self.sim_time = 0

        self.prediction_sw = SimpleStopwatch()

    def fit(self, data, items=None):
        print('vsknn.fit() called')
        data.sort_values(['SessionId', 'Time'], inplace=True)
        data = data.reset_index(drop=True)

        self.training_item_ids = data['ItemId'].unique()

        train = data

        index_session = train.columns.get_loc(self.session_key)
        index_item = train.columns.get_loc(self.item_key)
        index_time = train.columns.get_loc(self.time_key)

        session = -1
        session_items = set()
        time = -1
        for row in train.itertuples(index=False):
            if row[index_session] != session:
                if len(session_items) > 0:
                    self.session_item_map.update({session: session_items})
                    self.session_time.update({session: time})
                    if time < self.min_time:
                        self.min_time = time
                session = row[index_session]
                session_items = set()
            time = row[index_time]
            session_items.add(row[index_item])

            # cache sessions involving an item
            map_is = self.item_session_map.get(row[index_item])
            if map_is is None:
                map_is = set()
                self.item_session_map.update({row[index_item]: map_is})
            map_is.add(row[index_session])

        # Add the last tuple
        self.session_item_map.update({session: session_items})
        self.session_time.update({session: time})

        if self.idf_weighting or self.idf_weighting_session:
            print('start idf')
            self.idf = pd.DataFrame()
            self.idf['idf'] = train.groupby(self.item_key).size()
            self.idf['idf'] = np.log(train[self.session_key].nunique() / self.idf['idf'])
            self.idf['idf'] = self.idf['idf'] * self.idf_weighting
            self.idf = self.idf['idf'].to_dict()

    def predict_next(self, session_id, input_item_id, timestamp=0, skip=False, type='view'):
        '''
        Gives predicton scores for a selected set of items on how likely they be the next item in the session.

        Parameters
        --------
        session_id : int or string
            The session IDs of the event.
        input_item_id : int or string
            The item ID of the event. Must be in the set of item IDs of the training set.

        Returns
        --------
        out : pandas.Series
            Prediction scores for selected items on how likely to be the next item of this session. Indexed by the item IDs.

        '''
        self.prediction_sw.start()
        if (self.session != session_id):  # new session
            self.last_ts = -1
            self.session = session_id
            self.session_items = list()
            self.dwelling_times = list()
            self.relevant_sessions = set()

        self.session_items.append(input_item_id)

        possible_neighbors = self.possible_neighbor_sessions(self.session_items, input_item_id, session_id)
        possible_neighbors = self.calc_similarity(self.session_items, possible_neighbors, self.dwelling_times, timestamp)
        scores = self.score_items(possible_neighbors, self.session_items, timestamp)

        # Create things in the format ..
        predictions = np.zeros(len(self.training_item_ids))
        mask = np.in1d(self.training_item_ids, list(scores.keys()))
        items = self.training_item_ids[mask]
        values = [scores[x] for x in items]
        predictions[mask] = values
        series = pd.Series(data=predictions, index=self.training_item_ids)
        series[input_item_id] = 0
        if self.normalize:
            series = series / series.max()
        series[np.isnan(series)] = 0
        series.sort_values(ascending=False, inplace=True)
        self.prediction_sw.stop(len(self.session_items))
        return series

    @staticmethod
    def vec(first, second, map):
        a = first & second
        sum = 0
        for i in a:
            sum += map[i]

        return sum

    def most_recent_sessions(self, sessions, number):
        tuples = list()
        for session in sessions:
            time = self.session_time.get(session)
            if time is None:
                print(' EMPTY TIMESTAMP!! ', session)
            tuples.append((session, time))

        tuples = sorted(tuples, key=itemgetter(1), reverse=True)[:number]
        sample = set(session for (session, _) in tuples)
        return sample

    def possible_neighbor_sessions(self, session_items, input_item_id, session_id):
        self.relevant_sessions = self.relevant_sessions | self.item_session_map.get(input_item_id, set())

        if len(self.relevant_sessions) > self.sample_size:
            sample = self.most_recent_sessions(self.relevant_sessions, self.sample_size)
            return sample
        else:
            return self.relevant_sessions

    def calc_similarity(self, session_items, sessions, dwelling_times, timestamp):
        pos_map = {}
        items = set(session_items)
        qty_distinct_items = len(items)

        count = 1
        for item in session_items:
            pos_map[item] = count / qty_distinct_items
            count += 1

        neighbors = []
        for session in sessions:
            # get items of the session, look up the cache first
            n_items = self.session_item_map.get(session)
            # dot product
            similarity = self.vec(items, n_items, pos_map)
            if similarity > 0:
                neighbors.append((session, similarity))

        neighbors = sorted(neighbors, key=itemgetter(1), reverse=True)[:self.k]
        return neighbors

    def score_items(self, neighbors, current_session, timestamp):
        scores = dict()
        qty_unique_items_in_evolving_session = len(set(current_session))
        # iterate over the sessions
        for (h_session_id, h_session_score) in neighbors:
            h_session_score = h_session_score / qty_unique_items_in_evolving_session  # reduce session importance weight with every newly interacted item
            # get the items in this historical session
            items = self.session_item_map.get(h_session_id)
            step = 0

            for item in reversed(current_session):
                step += 1
                if item in items:
                    decay = 1 - (0.1 * step)
                    break

            if step <= 100:
                decayed_session_score = h_session_score * decay
                for item in items:
                    # new_score = h_session_score + (h_session_score * self.idf[item])
                    new_score = decayed_session_score * self.idf[item]

                    sum_of_scores = scores.get(item, 0)
                    new_score = sum_of_scores + new_score

                    scores.update({item: new_score})
        return scores

    def get_latencies(self):
        return self.prediction_sw.get_prediction_latencies_in_micros()
