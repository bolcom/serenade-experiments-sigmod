class Popularity:
    '''
    Popularity( length=20 )

    Used to iteratively calculate the average overall popularity of an algorithm's recommendations. 

    Parameters
    -----------
    length : int
        Coverage@length
    training_df : dataframe
        determines how many distinct item_ids there are in the training data
    '''
    
    def __init__(self, length=20, training_df=None):
        self.length = length;
        self.sum = 0
        self.tests = 0
        self.train_actions = len(training_df.index)
        #group the data by the itemIds
        grp = training_df.groupby('ItemId')
        #count the occurence of every itemid in the trainingdataset
        self.pop_scores = grp.size()
        #sort it according to the score
        self.pop_scores.sort_values(ascending=False, inplace=True)
        #normalize
        self.pop_scores = self.pop_scores / self.pop_scores[:1].values[0]


    def add(self, result, next_items, for_item=0, session=0, pop_bin=None, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        result: pandas.Series
            Series of scores with the item id as the index
        '''
        #only keep the k- first predictions
        recs = result[:self.length]
        #take the unique values out of those top scorers
        items = recs.index.unique()
                
        self.sum += ( self.pop_scores[ items ].sum() / len( items ) )
        self.tests += 1
        
    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("Popularity@" + str( self.length ) + ": "), ( self.sum / self.tests )
        