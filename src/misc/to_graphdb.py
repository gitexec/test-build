# #!/usr/bin/env python
# # coding: utf-8

# # In[72]:


# from neomodel import core
# from neomodel.properties import *
# from neomodel import RelationshipTo, RelationshipFrom, UniqueIdProperty
# import pandas as pd
# from neomodel import db
# from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,
#                       UniqueIdProperty, RelationshipTo, RelationshipFrom)
# from google.cloud import bigquery
# client = bigquery.Client.from_service_account_json(
#     'key.json')
# db.set_connection('bolt://neo4j:Xxw1sY62YHoN@localhost:7687')


# # In[69]:


# QUERY = """WITH cust AS (SELECT * FROM `raw.Customers`) select * from cust"""
# customers = pd.read_gbq(query=QUERY,
#                         project_id='front-icentris',
#                         private_key='key.json',
#                         dialect='standard')
# customers


# # In[3]:


# dt = customers.dtypes


# # In[ ]:


# # In[5]:


# def map_dtypes(name, columns, dtypes):
#     mapping = {'int64': 'IntegerProperty', 'object': 'StringProperty',
#                'bool': "BooleanProperty", 'float64': 'FloatProperty'}
#     mapping_class = 'class {}(StructuredNode):'.format(name)
#     for col, prop in zip(columns, dtypes):
#         dest_type = mapping.get(prop.name)
#         mapping_class += '\n    {} = {}()'.format(col.lower(), dest_type)
#     return mapping_class


# # In[6]:

# cust_class = map_dtypes('Customer', customers.columns, customers.dtypes)


# # In[7]:


# print(cust_class)


# # In[38]:


# class Customer(StructuredNode):
#     uuid = UniqueIdProperty()
#     customerid = IntegerProperty(unique_index=True)
#     firstname = StringProperty()
#     middlename = StringProperty()
#     lastname = StringProperty()
#     namesuffix = StringProperty()
#     company = StringProperty()
#     customertypeid = IntegerProperty()
#     customerstatusid = IntegerProperty()
#     email = StringProperty(unique_index=True)
#     phone = StringProperty()
#     phone2 = StringProperty()
#     mobilephone = StringProperty()
#     fax = StringProperty()
#     mainaddress1 = StringProperty()
#     mainaddress2 = StringProperty()
#     mainaddress3 = StringProperty()
#     maincity = StringProperty()
#     mainstate = StringProperty()
#     mainzip = StringProperty()
#     maincountry = StringProperty()
#     maincounty = StringProperty()
#     mainverified = BooleanProperty()
#     mailaddress1 = StringProperty()
#     mailaddress2 = StringProperty()
#     mailaddress3 = StringProperty()
#     mailcity = StringProperty()
#     mailstate = StringProperty()
#     mailzip = StringProperty()
#     mailcountry = StringProperty()
#     mailcounty = StringProperty()
#     mailverified = BooleanProperty()
#     otheraddress1 = StringProperty()
#     otheraddress2 = StringProperty()
#     otheraddress3 = StringProperty()
#     othercity = StringProperty()
#     otherstate = StringProperty()
#     otherzip = StringProperty()
#     othercountry = StringProperty()
#     othercounty = StringProperty()
#     otherverified = BooleanProperty()
#     canlogin = BooleanProperty()
#     loginname = StringProperty()
#     passwordhash = StringProperty()
#     rankid = FloatProperty()
#     enrollerid = IntegerProperty(unique_index=True)
#     sponsorid = IntegerProperty(unique_index=True)
#     birthdate = FloatProperty()
#     currencycode = StringProperty()
#     payabletoname = StringProperty()
#     defaultwarehouseid = FloatProperty()
#     payabletypeid = IntegerProperty()
#     checkthreshold = StringProperty()
#     languageid = FloatProperty()
#     gender = StringProperty()
#     taxcode = StringProperty()
#     taxcodetypeid = IntegerProperty()
#     issalestaxexempt = BooleanProperty()
#     salestaxcode = StringProperty()
#     salestaxexemptexpiredate = StringProperty()
#     vatregistration = StringProperty()
#     binaryplacementtypeid = IntegerProperty()
#     usebinaryholdingtank = BooleanProperty()
#     isinbinaryholdingtank = BooleanProperty()
#     isemailsubscribed = BooleanProperty()
#     emailsubscribeip = StringProperty()
#     issmssubscribed = BooleanProperty()
#     notes = StringProperty()
#     field1 = StringProperty()
#     field2 = StringProperty()
#     field3 = StringProperty()
#     field4 = StringProperty()
#     field5 = StringProperty()
#     field6 = StringProperty()
#     field7 = StringProperty()
#     field8 = StringProperty()
#     field9 = StringProperty()
#     field10 = StringProperty()
#     field11 = StringProperty()
#     field12 = StringProperty()
#     field13 = StringProperty()
#     field14 = StringProperty()
#     field15 = StringProperty()
#     date1 = FloatProperty()
#     date2 = StringProperty()
#     date3 = StringProperty()
#     date4 = StringProperty()
#     date5 = StringProperty()
#     createddate = IntegerProperty()
#     modifieddate = IntegerProperty()
#     createdby = StringProperty()
#     modifiedby = StringProperty()
#     emailunsubscribedate = FloatProperty()
#     emailsubscribedate = FloatProperty()
#     smssubscribedate = StringProperty()
#     smsunsubscribedate = StringProperty()
#     sponsored_by = RelationshipFrom('Customer', 'sponsored_by')
#     sponsoring = RelationshipTo('Customer', 'sponsoring')


# # In[37]:


# #del db._NODE_CLASS_REGISTRY[frozenset({'Customer'})]
# # core.drop_indexes()
# core.remove_all_labels()


# # In[71]:


# for cust in Customer.nodes.all():
#     cust.delete()


# # In[70]:


# for index, row in customers.iterrows():
#     try:
#         properties = {col.lower(): getattr(row, col) for col in customers.columns}
#         customer = Customer(**properties)
#         customer.save()
#     except Exception as e:
#         print(row)
#         raise(e)


# # In[68]:


# for cust in Customer.nodes.all():
#     cust.delete()


# # In[64]:


# all_cust = Customer.nodes.all()
# cust = all_cust[0]
# cust


# # In[ ]:


# QUERY = """WITH cust AS (SELECT * FROM `raw.UniLevelTree`) select * from cust where customerid"""
# tree = pd.read_gbq(query=QUERY,
#                    project_id='front-icentris',
#                    private_key='key.json',
#                    dialect='standard')
# tree


# # In[ ]:


# for index, row in tree.iterrows():
#     id = row['CustomerID']
#     sponsor_id = row['SponsorID']
#     left = row['Lft']
#     right = row['Rgt']
#     try:
#         if id == 0:
#             continue
#         customer = Customer.nodes.get(customerid=id)
#         sponsor = Customer.nodes.get_or_none(customerid=sponsor_id)
#         if sponsor:
#             customer.sponsored_by.connect(sponsor)
#         left_sub = Customer.nodes.get_or_none(customerid=left)
#         right_sub = Customer.nodes.get_or_none(customerid=right)
#         if left_sub:
#             customer.sponsoring.connect(left_sub)
#         if right_sub:
#             customer.sponsoring.connect(right_sub)
#     except Exception as e:
#         print("ROW IS : {}".format(row))
#         raise(e)


# # In[65]:


# customer = Customer.nodes.get(customerid=3)


# # In[67]:


# customer.sponsored_by.all()


# # In[ ]:
