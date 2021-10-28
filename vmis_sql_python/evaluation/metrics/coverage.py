
class Coverage:
    '''
    Coverage( length=20 )

    Used to iteratively calculate the coverage of an algorithm regarding the item space. 

    Parameters
    -----------
    length : int
        Coverage@length
    training_df : dataframe
        determines how many distinct item_ids there are in the training data
    '''
    
    def __init__(self, length=20, training_df=None):
        self.num_items = 0
        self.length = length
        self.time = 0
        self.num_items = len(training_df['ItemId'].unique())
        self.coverage_set = set()


    def add(self, recommendations, next_items, for_item=0, session=0, pop_bin=None, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        recs = recommendations[:self.length]
        items = recs.index.unique()
        self.coverage_set.update( items )

        
    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("Coverage@" + str(self.length) + ": "), ( len(self.coverage_set) / self.num_items )
    