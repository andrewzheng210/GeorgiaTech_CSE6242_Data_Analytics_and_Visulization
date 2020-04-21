import numpy as np
import ast
from util import entropy, information_gain, partition_classes

class DecisionTree(object):
    def __init__(self, max_depth=10):
        # Initializing the tree as an empty dictionary or list, as preferred
        self.tree = {}
        self.max_depth = max_depth

    def learn(self, X, y, par_node = {}, depth=0):
        # TODO: Train the decision tree (self.tree) using the the sample X and labels y
        # You will have to make use of the functions in utils.py to train the tree

        # Use the function best_split in util.py to get the best split and
        # data corresponding to left and right child nodes

        # One possible way of implementing the tree:
        #    Each node in self.tree could be in the form of a dictionary:
        #       https://docs.python.org/2/library/stdtypes.html#mapping-types-dict
        #    For example, a non-leaf node with two children can have a 'left' key and  a
        #    'right' key. You can add more keys which might help in classification
        #    (eg. split attribute and split value)
        ### Implement your code here
        #############################################
        self.group = None
        #self.depth = 0

        max_info_gain = -float("inf")
        split_attribute = 0
        x_l, x_r, y_l, y_r = [], [], [], []

        if self.max_depth > 0 and entropy(y) > 0:

            # Import best_split function caused unkown bug. So re-write best_split function here.
            # Select m variables out of all variables
            m = int(np.sqrt(len(X[0])))
            column_index_array = np.random.randint(low=0, high=len(X[0]), size=m)

            for col in column_index_array:
                if not isinstance(X[0][col], str):
                    values = [row[col] for row in X]
                    cur_split_val = sum(values)/len(values)
                    # For the selected variable, sometiems all data have the same data, then return to its node.
                    if [cur_split_val] * len(values) == values:
                        self.group = y[0]
                        return

                    X_left, X_right, y_left, y_right = partition_classes(X, y, col, cur_split_val)
                    cur_y = [y_left, y_right]
                    cur_info_gain = information_gain(y, cur_y)
                    if max_info_gain < cur_info_gain:
                        max_info_gain = cur_info_gain
                        x_l, x_r, y_l, y_r = X_left, X_right, y_left, y_right
                        split_attribute = col
                        split_val = cur_split_val
                else:
                    var_set = set([x[col] for x in X])
                    # For the selected variable, sometiems all data have the same data, then return to its node.
                    if len(var_set) ==1:
                        self.group = y[0]
                        return
                    for item in var_set:
                        X_left, X_right, y_left, y_right= partition_classes(X, y, col, item)
                        cur_info_gain = information_gain(y, [y_left,y_right])
                        if max_info_gain < cur_info_gain:
                            max_info_gain = cur_info_gain
                            x_l, x_r, y_l, y_r = X_left, X_right, y_left, y_right
                            split_attribute = col
                            split_val = item

            #print('group is',self.group)
            #print('max_depth is',self.max_depth)
            self.tree['left'], self.tree['right'] = DecisionTree(max_depth=self.max_depth-1), DecisionTree(max_depth=self.max_depth-1)
            self.tree['split_attribute'] = split_attribute
            #print('split_atrribute is',self.tree['split_attribute'])
            self.tree['split_val'] = split_val
            #print(x_l, y_l, x_r, y_r)
            self.tree['left'].learn(x_l, y_l)
            #print('left is done')
            self.tree['right'].learn(x_r, y_r)
            #print('right is done')

        else:
            self.group = y[0]
            #print('group is',self.group)
            return

        #############################################


    def classify(self, record):
        # TODO: classify the record using self.tree and return the predicted label
        ### Implement your code here
        #############################################
        node = self.tree
        if self.group != None:
            return self.group
        while self.group == None:
            split_val = node['split_val']
            split_attribute = node['split_attribute']

            if isinstance(record[split_attribute], str):
                if record[split_attribute] == split_val:
                    label = node['left'].classify(record)
                else:
                    label = node['right'].classify(record)
            else:
                if record[split_attribute] <= split_val:
                    label = node['left'].classify(record)
                else:
                    label = node['right'].classify(record)
            return label
        #############################################
