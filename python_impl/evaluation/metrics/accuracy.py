import numpy as np

class MRR: 
    '''
    MRR( length=20 )

    Used to iteratively calculate the average mean reciprocal rank for a result list with the defined length. 

    Parameters
    -----------
    length : int
        MRR@length
    '''
    def __init__(self, length=20):
        self.length = length
        self.test = 0;
        self.pos = 0

        self.test_popbin = {}
        self.pos_popbin = {}

        self.test_position = {}
        self.pos_position = {}

    def add(self, recommendations, next_items, for_item=0, session=0, pop_bin=None, position=None):
        next_item = next_items[0]
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series the recommendations for for_item.
            Series of scores with the item id as the index
        next_items: the next items in the session the person will click on
        '''
        res = recommendations[:self.length]
        
        self.test += 1
        
        if pop_bin is not None:
            if pop_bin not in self.test_popbin:
                self.test_popbin[pop_bin] = 0
                self.pos_popbin[pop_bin] = 0
            self.test_popbin[pop_bin] += 1
        
        if position is not None:
            if position not in self.test_position:
                self.test_position[position] = 0
                self.pos_position[position] = 0
            self.test_position[position] += 1
        
        if next_item in res.index:
            rank = res.index.get_loc( next_item )+1
            self.pos += ( 1.0/rank )
            
            if pop_bin is not None:
                self.pos_popbin[pop_bin] += ( 1.0/rank )
            
            if position is not None:
                self.pos_position[position] += ( 1.0/rank )

    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("MRR@" + str(self.length) + ": "), (self.pos/self.test)
    
    def result_pop_bin(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        csv = ''
        csv += 'Bin: ;'
        for key in self.test_popbin:
            csv += str(key) + ';'
        csv += '\nPrecision@' + str(self.length) + ': ;'
        for key in self.test_popbin:
            csv += str( self.pos_popbin[key] / self.test_popbin[key] ) + ';'
            
        return csv
    
    def result_position(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        csv = ''
        csv += 'Pos: ;'
        for key in self.test_position:
            csv += str(key) + ';'
        csv += '\nPrecision@' + str(self.length) + ': ;'
        for key in self.test_position:
            csv += str( self.pos_position[key] / self.test_position[key] ) + ';'
            
        return csv
    
class HitRate: 
    '''
    MRR( length=20 )

    Used to iteratively calculate the average hit rate for a result list with the defined length. 

    Parameters
    -----------
    length : int
        HitRate@length
    '''
    
    def __init__(self, length=20):
        self.length = length
        self.test = 0;
        self.hit = 0

        self.test_popbin = {}
        self.hit_popbin = {}

        self.test_position = {}
        self.hit_position = {}

    def add(self, recommendations, next_items, for_item=0, session=0, pop_bin=None, position=None):
        '''
        Update the metric with a result set and the correct next item.
        Result must be sorted correctly.
        
        Parameters
        --------
        recommendations: pandas.Series
            Series of scores with the item id as the index
        '''
        next_item = next_items[0]
        self.test += 1
         
        if pop_bin is not None:
            if pop_bin not in self.test_popbin:
                self.test_popbin[pop_bin] = 0
                self.hit_popbin[pop_bin] = 0
            self.test_popbin[pop_bin] += 1
        
        if position is not None:
            if position not in self.test_position:
                self.test_position[position] = 0
                self.hit_position[position] = 0
            self.test_position[position] += 1
                
        if next_item in recommendations[:self.length].index:
            self.hit += 1
            
            if pop_bin is not None:
                self.hit_popbin[pop_bin] += 1
            
            if position is not None:
                self.hit_position[position] += 1

        
    def result(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        return ("HitRate@" + str(self.length) + ": "), (self.hit/self.test)

    
    def result_pop_bin(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        csv = ''
        csv += 'Bin: ;'
        for key in self.test_popbin:
            csv += str(key) + ';'
        csv += '\nHitRate@' + str(self.length) + ': ;'
        for key in self.test_popbin:
            csv += str( self.hit_popbin[key] / self.test_popbin[key] ) + ';'
            
        return csv
    
    def result_position(self):
        '''
        Return a tuple of a description string and the current averaged value
        '''
        csv = ''
        csv += 'Pos: ;'
        for key in self.test_position:
            csv += str(key) + ';'
        csv += '\nHitRate@' + str(self.length) + ': ;'
        for key in self.test_position:
            csv += str( self.hit_position[key] / self.test_position[key] ) + ';'
            
        return csv
