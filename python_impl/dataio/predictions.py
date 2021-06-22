import numpy as np
import pandas as pd

class PredictionsWriter:
    def __init__(self, outputfilename, evaluation_n=20):
        self.file_handler = open(outputfilename, 'w')
        self.evaluation_n = evaluation_n

    def appendline(self, predictions, next_items):
        # predictions a pandas series. expected to be sorted by value. Index = item_id, the value = score.
        # next_items a numpy array. values represent the item_ids.
        top_n_prediction_ids = ','.join(str(index) for index in predictions[:self.evaluation_n].keys().tolist())
        next_items = ','.join(str(index) for index in next_items)
        self.file_handler.write("{top_n_prediction_ids};{next_items}\n".format(top_n_prediction_ids=top_n_prediction_ids,
                                                                         next_items=next_items))

    def close(self):
        self.file_handler.close()


class PredictionsReader:
    '''
    read csv files for evaluating session based predictions from files.

    Fileformat:
    Each line contains the predicted recommendations and the actual next_items that a user is going to interact with during the same session.
    We only need the item_ids in the csv file. We leave out the session_id and prediction scores to reduce file size.
    recommendation_ids;next_item_ids
    5226,72773,76493,23152,8972,37154,6124,11075;76493,8972
    5226 being the highest scored recommendation.
    76493 is the next_item that will be interacted with.

    Since the evaluation is @20 at most, we only need to store the top-20 recommendations for evaluation.
    All the future next_item_ids that will be interacted with in the session from must be stored.
    '''


    def __init__(self, inputfilename, training_df):
        self.file_handler = open(inputfilename, 'r')
        self.training_item_ids = training_df['ItemId'].unique()

    def get_next_line(self):
        for line in self.file_handler:
            raw_recos, raw_next_items = line.rstrip('\n').split(';', 1)
            recos = raw_recos.split(',')
            series = self.__raw_recos_to_series(recos)
            next_items = np.array(raw_next_items.split(',')).astype(int)
            # convert everything back in its original format
            yield series, next_items

    def __raw_recos_to_series(self, recos):
        scores = {}
        max_score = 1.0000
        for reco_id in recos:
            scores[int(reco_id)] = max_score
            max_score = max_score - 0.001

        # Create things in the inefficient dense format ..
        predictions = np.zeros(len(self.training_item_ids))
        mask = np.in1d(self.training_item_ids, list(scores.keys()))
        items = self.training_item_ids[mask]
        values = [scores[x] for x in items]
        predictions[mask] = values
        series = pd.Series(data=predictions, index=self.training_item_ids)
        series = series / series.max()
        series[np.isnan(series)] = 0
        series.sort_values(ascending=False, inplace=True)
        return series

