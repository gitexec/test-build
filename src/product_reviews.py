#!/usr/bin/env python
# coding: utf-8

# In[1]:


from collections import defaultdict
from surprise.model_selection import KFold
from surprise import accuracy
from surprise import SVD
# from surprise.model_selection import cross_validate
from surprise import Reader
from surprise import Dataset
# from surprise import NormalPredictor
import pandas as pd
# get_ipython().system('gsutil cp gs://icentris-data-model-ml-d1/product-reviews.csv .')


# In[8]:


# In[4]:


reviews = pd.read_csv('product-reviews.csv', names=['product_id', 'user_id', 'rating'])


# In[7]:


reader = Reader(rating_scale=(1, 5))
dataset = Dataset.load_from_df(reviews[['user_id', 'product_id', 'rating']], reader)


# In[9]:


kf = KFold(n_splits=3)

algo = SVD()
for trainset, testset in kf.split(dataset):

    # train and test algorithm.
    algo.fit(trainset)
    predictions = algo.test(testset)

    # Compute and print Root Mean Squared Error
    accuracy.rmse(predictions, verbose=True)


# In[10]:


def get_top_n(predictions, n=5):
    '''Return the top-N recommendation for each user from a set of predictions.

    Args:
        predictions(list of Prediction objects): The list of predictions, as
            returned by the test method of an algorithm.
        n(int): The number of recommendation to output for each user. Default
            is 10.

    Returns:
    A dict where keys are user (raw) ids and values are lists of tuples:
        [(raw item id, rating estimation), ...] of size n.
    '''

    # First map the predictions to each user.
    top_n = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_n[uid].append((iid, est))

    # Then sort the predictions for each user and retrieve the k highest ones.
    for uid, user_ratings in top_n.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        top_n[uid] = user_ratings[:n]

    return top_n


# In[11]:


top_n = get_top_n(predictions)


# In[13]:


# predicted rating for top 5 products for each user
top_n


# In[ ]:
