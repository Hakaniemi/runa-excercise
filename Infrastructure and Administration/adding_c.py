import pandas as pd
import boto3

BUCKET_NAME = 'bhakaniemi'
REGION = 'us-east-2'
ACCESS_KEY_ID = 'AKIAJ5GTIZMFXECFNGKQ'
SECRET_ACCESS_KEY = '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'


# gets connection to aws/s3
session = boto3.Session(profile_name='default')
client = boto3.client('s3')
s3 = boto3.resource('s3')


# loading file .csv to crete dataframe
pls = pd.read_csv('s3:/bhakaniemi/Players_2020.csv', index_col = 'Name')
print(pls)
print('\n\n')

#adding new column, calculating ratio using columns Weak Foot, Skill Moves
(pls.assign(performance_ratio=pls['Weak Foot'] / pls['Skill Moves']))
pls2 = (pls.assign(performance_ratio=pls['Weak Foot'] / pls['Skill Moves']))
print(pls2)
print('\n\n')

#Uploading new csv  to s3
pls2.to_csv('s3:/bhakaniemi/Players2020_add.csv')
pls2 = pd.read_csv('/Users/Hakaniemi1/PycharmProjects/pythonProject/Players2020_add.csv', index_col = 'Name')
print('\n\n')

#Comparing dataframe pls and pls2
comp = pls.ne(pls2)
print('Comparing tables player and player 2')
print(comp)
print('\n\n')

#Comparing dtypes for dataframe columns ID and Nationality
print('Comparing dtypes')
if pls.ID.dtype == pls.Nationality.dtype:
    print('Equal data type')
else:
    print('Not equal data type')

print('\n\n')


