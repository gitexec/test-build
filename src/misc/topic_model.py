# #!/usr/bin/env python
# # coding: utf-8

# # In[1]:


# from multiprocessing import Pool
# from nltk.corpus import wordnet as wn
# from spacy.lang.en import English
# from nltk.stem.wordnet import WordNetLemmatizer
# import nltk
# import en_core_web_md
# import spacy
# import pymysql
# from pymysql import cursors
# c = pymysql.connect(host='10.92.32.3', user='root', password='icentris', db='exigo', cursorclass=cursors.DictCursor)
# results = []
# with c.cursor() as cursor:
#     cursor.execute('select SUBJECT, message, message_id from messages')
#     results = cursor.fetchall()


# # In[2]:


# nlp = en_core_web_md.load()
# parser = English()

# nltk.download('wordnet')


# # In[3]:


# def get_lemma(word):
#     lemma = wn.morphy(word)
#     if lemma is None:
#         return word
#     else:
#         return lemma


# def get_lemma2(word):
#     return WordNetLemmatizer().lemmatize(word)


# def tokenize(text):
#     lda_tokens = []
#     tokens = parser(text)
#     for token in tokens:
#         if token.orth_.isspace():
#             continue
#         elif token.like_url:
#             lda_tokens.append('URL')
#         elif token.orth_.startswith('@'):
#             lda_tokens.append('SCREEN_NAME')
#         else:
#             lda_tokens.append(token.lower_)
#     return lda_tokens


# nltk.download('stopwords')
# en_stop = set(nltk.corpus.stopwords.words('english'))


# def prepare_text_for_lda(text):
#     tokens = tokenize(text)
#     tokens = [get_lemma(token) for token in tokens if len(token) > 4 and token not in en_stop]
#     return tokens


# # In[5]:


# pool = Pool(4)


# # In[ ]:


# msg_tokens = pool.map(prepare_text_for_lda,
#                       [msg['message'] for msg in results if msg['message'] and len(msg['message']) > 1])
# print(msg_tokens[:10])


# # In[ ]:


# sub_tokens = pool.map(prepare_text_for_lda,
#                       [msg['subject'] for msg in results if msg['subject'] and len(msg['subject']) > 1])
# print(sub_tokens[:10])


# # In[ ]:
