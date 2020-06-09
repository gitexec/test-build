# #!/usr/bin/env python
# # coding: utf-8

# # In[2]:


# from gensim.models.phrases import Phrases, Phraser
# from pyLDAvis import _prepare
# import pyLDAvis.gensim
# import pyLDAvis
# from pprint import pprint
# from gensim import corpora, models
# from spacy.lang.en import English
# import nltk
# import numpy as np
# from nltk.stem.porter import *
# from nltk.stem import WordNetLemmatizer, SnowballStemmer
# from gensim.parsing.preprocessing import STOPWORDS
# from gensim.utils import simple_preprocess
# import gensim
# import pickle
# with open('message_failed_tokens.pickle', 'rb') as f:
#     failed = pickle.load(f)
# with open('message_success_tokens.pickle', 'rb') as f:
#     success = pickle.load(f)


# # In[11]:


# type(messages)


# # In[3]:


# np.random.seed(2018)
# nltk.download('wordnet')
# #nlp = en_core_web_md.load()
# parser = English()


# # In[4]:


# dictionary = gensim.corpora.Dictionary(failed + success)
# count = 0
# for k, v in dictionary.iteritems():
#     print(k, v)
#     count += 1
#     if count > 10:
#         break


# # In[5]:


# dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)


# # In[7]:


# all = failed + success


# # In[8]:


# bow_corpus_fail = [dictionary.doc2bow(doc) for doc in failed]
# bow_corpus_success = [dictionary.doc2bow(doc) for doc in success]


# # In[11]:


# tfidf_fail = models.TfidfModel(bow_corpus_fail)
# tfidf_success = models.TfidfModel(bow_corpus_success)
# corpus_tfidf_fail = tfidf_fail[bow_corpus_fail]
# corpus_tfidf_success = tfidf_success[bow_corpus_success]
# for doc in corpus_tfidf_success:
#     pprint(doc)
#     break


# # In[12]:


# lda_model_fail = gensim.models.LdaMulticore(bow_corpus_fail, num_topics=10, id2word=dictionary, passes=2, workers=2)
# lda_model_success = gensim.models.LdaMulticore(
#     bow_corpus_success, num_topics=10, id2word=dictionary, passes=2, workers=2)


# # In[13]:


# for idx, topic in lda_model_success.print_topics(-1):
#     print('Topic: {} \nWords: {}'.format(idx, topic))


# # In[14]:


# lda_model_tfidf_fail = gensim.models.LdaMulticore(
#     corpus_tfidf_fail, num_topics=10, id2word=dictionary, passes=2, workers=4)
# lda_model_tfidf_success = gensim.models.LdaMulticore(
#     corpus_tfidf_success, num_topics=10, id2word=dictionary, passes=2, workers=4)
# for idx, topic in lda_model_tfidf_fail.print_topics(-1):
#     print('Topic: {} Word: {}'.format(idx, topic))


# # In[16]:


# for index, score in sorted(lda_model_success[bow_corpus_success[4310]], key=lambda tup: -1*tup[1]):
#     print("\nScore: {}\t \nTopic: {}".format(score, lda_model_success.print_topic(index, 10)))


# # In[ ]:


# for index, score in sorted(lda_model_tfidf_success[bow_corpus_success[4310]], key=lambda tup: -1*tup[1]):
#     print("\nScore: {}\t \nTopic: {}".format(score, lda_model_tfidf.print_topic(index, 10)))


# # In[27]:


# pyLDAvis.enable_notebook()
# pyLDAvis.utils


# # In[19]:


# corpus_success = [dictionary.doc2bow(text) for text in success]
# extracted_vis_success = pyLDAvis.gensim._extract_data(lda_model_success, corpus_success, dictionary)


# # In[23]:


# corpus_fail = [dictionary.doc2bow(text) for text in failed]
# extracted_vis_fail = pyLDAvis.gensim._extract_data(lda_model_fail, corpus_fail, dictionary)


# # In[28]:


# pyLDAvis.enable_notebook()
# #extracted = pyLDAvis.gensim._extract_data(lda_model_tfidf, corpus, dictionary)
# vis_success = _prepare.prepare(n_jobs=8, **extracted_vis_success)


# # In[29]:


# pyLDAvis.enable_notebook()
# #extracted = pyLDAvis.gensim._extract_data(lda_model_tfidf, corpus, dictionary)
# vis_fail = _prepare.prepare(n_jobs=8, **extracted_vis_fail)


# # In[30]:


# with open('lda_success.pickle', 'wb') as f:
#     pickle.dump(vis_success, f)
# with open('lda_fail.pickle', 'wb') as f:
#     pickle.dump(vis_fail, f)


# # In[31]:


# pyLDAvis.display(vis_success)


# # In[32]:


# pyLDAvis.display(vis_fail)


# # In[38]:


# extracted_vis_fail_tfidf = pyLDAvis.gensim._extract_data(lda_model_tfidf_fail, corpus_fail, dictionary)
# vis_fail_tfidf = _prepare.prepare(n_jobs=8, **extracted_vis_fail_tfidf)


# # In[39]:


# extracted_vis_success_tfidf = pyLDAvis.gensim._extract_data(lda_model_tfidf_success, corpus_success, dictionary)
# vis_success_tfidf = _prepare.prepare(n_jobs=8, **extracted_vis_success_tfidf)


# # In[41]:


# pyLDAvis.display(vis_fail_tfidf)


# # In[40]:


# pyLDAvis.display(vis_success_tfidf)


# # In[42]:


# phrases = Phrases(all)


# # In[43]:


# trigrams = Phrases(phrases[all])


# # In[48]:


# bi_dictionary = gensim.corpora.Dictionary([phrases[msg] for msg in all])
# tri_dict = gensim.corpora.Dictionary([trigrams[msg] for msg in all])


# # In[46]:


# bi_dictionary_fail = gensim.corpora.Dictionary([phrases[msg] for msg in failed])
# tri_dict_fail = gensim.corpora.Dictionary([trigrams[msg] for msg in failed])


# # In[50]:


# #print("{} -- {}".format(dictionary[10], bi_dictionary[10]))
# bow_corpus_success = [bi_dictionary.doc2bow(doc) for doc in [phrases[msg] for msg in success]]
# bow_corpus_fail = [bi_dictionary.doc2bow(doc) for doc in [phrases[msg] for msg in failed]]
# tfidf_success = models.TfidfModel(bow_corpus_success)
# tfidf_failed = models.TfidfModel(bow_corpus_fail)
# corpus_tfidf_success = tfidf_success[bow_corpus_success]
# corpus_tfidf_failed = tfidf_failed[bow_corpus_fail]


# # In[55]:


# lda_model_tfidf_success = gensim.models.LdaMulticore(
#     corpus_tfidf_success, num_topics=10, id2word=bi_dictionary, passes=2, workers=8)
# lda_bi_success = pyLDAvis.gensim.prepare(lda_model_tfidf_success, bow_corpus_success, bi_dictionary)


# # In[57]:


# pyLDAvis.display(lda_bi_success)


# # In[58]:


# lda_model_tfidf_fail = gensim.models.LdaMulticore(
#     corpus_tfidf_fail, num_topics=10, id2word=bi_dictionary, passes=2, workers=8)
# lda_bi_fail = pyLDAvis.gensim.prepare(lda_model_tfidf_fail, bow_corpus_fail, bi_dictionary)


# # In[59]:


# pyLDAvis.display(lda_bi_fail)


# # In[64]:


# with open('lda_tfidf_bigram_fail.html', 'w') as f:
#     pyLDAvis.save_html(lda_bi_fail, f)


# # In[ ]:
