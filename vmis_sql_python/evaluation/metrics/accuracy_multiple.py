import numpy as np
    
class Precision: 
    '''
    Precision( length=20 )

    Used to iteratively calculate the average precision for the the defined length.

    Parameters
    -----------
    length : int
        Precision@length
    '''
    
    def __init__(self, length=20):
        self.length = length
        self.test=0
        self.hit=0

    def add(self, recommendations, next_items, for_item=0, session=0, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        self.test += 1
        self.hit += len(set(next_items) & set(recommendations[:self.length].index)) / self.length

    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("Precision@" + str(self.length) + ": "), (self.hit/self.test)
    
class Recall: 
    '''
    Precision( length=20 )

    Used to iteratively calculate the average hit rate for a result list with the defined length. 

    Parameters
    -----------
    length : int
        HitRate@length
    '''
    
    def __init__(self, length=20):
        self.length = length
        self.test=0
        self.hit=0

    # def add(self, result, next_item, for_item=0, session=0, pop_bin=None, position=None):
    #     '''
    #     Update the metric with a result set and the correct next item.
    #     Result must be sorted correctly.
    #
    #     Parameters
    #     --------
    #     result: pandas.Series
    #         Series of scores with the item id as the index
    #     '''
    #     self.test += 1
    #     #a=set([next_item])
    #     #b=set(result[:self.length].index)
    #     #c=set([next_item]) & set(result[:self.length].index)
    #     self.hit += len( set([next_item]) & set(result[:self.length].index) )
       
    def add(self, recommendations, next_items, for_item=0, session=0, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        self.test += 1
        self.hit += len(set(next_items) & set(recommendations[:self.length].index)) / len(next_items)

    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("Recall@" + str(self.length) + ": "), (self.hit/self.test)


class MAP: 
    '''
    MAP( length=20 )

    Used to iteratively calculate the mean average precision for a result list with the defined length. 

    Parameters
    -----------
    length : int
        MAP@length
    '''
    def __init__(self, length=20):
        self.length = length
        self.test=0
        self.pos=0

    def add(self, recommendations, next_items, for_item=0, session=0, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        
        last_recall = 0
        
        res = 0
        
        for i in range(self.length):
            recall = self.recall(recommendations[:i].index, next_items)
            precision = self.precision(recommendations[:i].index, next_items)
            res += precision * (recall - last_recall)
            last_recall = recall
        
        self.pos += res
        self.test += 1

    def recall(self, recommendations, next_items):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        
        return len(set(next_items) & set(recommendations)) / len(next_items)
    
    def precision(self, recommendations, next_items):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        
        return len(set(next_items) & set(recommendations)) / self.length
    
    def mrr(self, recommendations, next_item, n):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        res = recommendations[:n]
        
        if next_item in res.index:
            rank = res.index.get_loc( next_item )+1
            return 1.0/rank
        else:
            return 0

    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("MAP@" + str(self.length) + ": "), (self.pos/self.test)

class NDCG:
    '''
    NDCG( length=20 )

    Used to iteratively calculate the Normalized Discounted Cumulative Gain for a result list with the defined length.

    Parameters
    -----------
    length : int
        NDCG@length
    '''

    def __init__(self, length=20):
        self.length = length
        self.test = 0;
        self.pos = 0

    def add(self, recommendations, next_items, for_item=0, session=0, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.

        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        dcg = self.dcg(recommendations[:self.length].index, next_items)
        dcg_max = self.dcg(next_items[:self.length], next_items)

        self.pos += dcg/dcg_max
        self.test += 1



    def dcg(self, recommendations, next_items):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.

        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''

        # relatedItems = list(set(result) & set(next_items))
        # for i in range(len(relatedItems)):
        #     idx = list(result).index(relatedItems[i])+1 #ranked position = index+1
        #     if idx == 1:
        #         res += rel
        #     else:
        #         res += rel / np.log2(idx)

        res = 0;
        rel = 1;
        ranked_list_len = min(len(recommendations), self.length)

        next_items = set(next_items)
        for i in range(ranked_list_len):          #range(self.length):
            if recommendations[i] in next_items:
                if i == 0:
                    res += rel
                else:
                    res += rel / np.log2(i+1)

        # res = rel[0]+np.sum(rel[1:] / np.log2(np.arange(2, rel.size + 1)))
        return res


    def sortFunc(e):
        return e.values;

    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("NDCG@" + str(self.length) + ": "), (self.pos/self.test)

