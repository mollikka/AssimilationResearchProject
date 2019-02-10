from __future__ import print_function

import sys
import os
from ast import literal_eval

import pandas
import numpy

import pudb

if len(sys.argv) > 2:
    DATAFRAME_FOLDER = sys.argv[1]
else:
    DATAFRAME_FOLDER = 'collected_data'

SPECIFIC_DATAFRAME_FOLDER = DATAFRAME_FOLDER + '/specific_data'
GLOBAL_DATAFRAME_FOLDER = DATAFRAME_FOLDER + '/global_data'

def get_global_interest_dict_id_to_name():

    global_interests = pandas.read_csv(GLOBAL_DATAFRAME_FOLDER + '/dataframe_interest_names.csv')

    interest_id_to_name = {}

    for index, row in global_interests.iterrows():
        interest_id_to_name[row['id']] = row['name']

    return interest_id_to_name

def get_global_interest_dict_id_to_audience():

    global_interests = pandas.read_csv(GLOBAL_DATAFRAME_FOLDER + '/dataframe_interest_names.csv')

    interest_id_to_audience = {}
    for index, row in global_interests.iterrows():
        interest_id_to_audience[row['id']] = row['audience_size']

    return interest_id_to_audience

def load_dataframes():
    #https://stackoverflow.com/questions/10545957/creating-pandas-data-frame-from-multiple-files

    #list the files
    filelist = os.listdir(SPECIFIC_DATAFRAME_FOLDER)
    #read them into pandas
    df_list = [pandas.read_csv(SPECIFIC_DATAFRAME_FOLDER+"/"+filename) for filename in filelist]
    #concatenate them together
    big_df = df_list[0].append(df_list[1:], sort=False)

    return big_df

def generate_helper_columns_on_dataframe(dataframe):

    location_ids = []
    interest_ids = []
    expat_statuses = []
    language_ids = []
    gender_ids = []
    for index, row in dataframe.iterrows():
        location_str = row['geo_locations']
        #change this if ever need to handle a query with multiple country codes
        location_id = literal_eval(location_str)['values']
        location_ids.append(location_id)

        interest_str = row['interests']
        interest_id = literal_eval(interest_str)['name']
        interest_ids.append(interest_id)

        behavior_str = row['behavior']
        expat_status = literal_eval(behavior_str)['name']
        expat_statuses.append(expat_status)

        gender_int = row['genders']
        if gender_int == 0:
            gender_ids.append('All')
        elif gender_int == 1:
            gender_ids.append('Male')
        elif gender_int == 2:
            gender_ids.append('Female')

        language_str = row['languages']
        if pandas.isnull(language_str):
            language_ids.append(None)
        else:
            language_id = literal_eval(language_str)['name']
            language_ids.append(language_id)

    dataframe = dataframe.assign(   interest_id=numpy.array(interest_ids),
                                    location_id=numpy.array(location_ids),
                                    expat      =numpy.array(expat_statuses),
                                    language_id=numpy.array(language_ids),
                                    gender_id  =numpy.array(gender_ids))

    return dataframe


data = load_dataframes()

data = generate_helper_columns_on_dataframe(data)

global_audience = get_global_interest_dict_id_to_audience()

def get_interest_vectors(data):

    vectors = []
    included = set()
    countries = data.location_id.unique()

    print('Found data for countries:', ','.join(countries))

    for country in countries:


        data_c = data[data['location_id'] == country]
        expat_statuses = data_c.expat.unique()

        print('\t',country,'has data for', ','.join(expat_statuses))

        for expat_status in expat_statuses:

            data_c_e = data_c[data_c['expat'] == expat_status]
            languages = data_c_e.language_id.unique()

            for language in languages:

                if language:
                    data_c_e_l = data_c_e[data_c_e['language_id'] == language]
                else:
                    data_c_e_l = data_c_e[data_c_e['language_id'].isnull()]

                genders = data_c_e_l.gender_id.unique()

                for gender in genders:

                    set_key = (country, expat_status, language or 'All', gender)
                    if set_key in included: continue
                    data_c_e_l_g = data_c_e_l[data_c_e_l['gender_id'] == gender]
                    data_c_e_l_g = data_c_e_l_g.drop_duplicates('interest_id',keep='first')
                    sorted_interests = data_c_e_l_g.sort_values('interest_id')
                    #sorted_interests_relative = [sorted_interests.iloc[i]['mau_audience']/float(global_audience[int(sorted_interests.iloc[i]['interest_id'])])
                    #                                if sorted_interests.iloc[i]['mau_audience']>1000 else None for i in range(len(sorted_interests))]
                    sorted_interests_relative = [sorted_interests.iloc[i]['mau_audience']/float(sum(sorted_interests['mau_audience']))
                                                    if sorted_interests.iloc[i]['mau_audience']>1000 else None for i in range(len(sorted_interests))]
                    interest_vector = [set_key, sorted_interests_relative]
                    included.add(set_key)
                    print('\t'*2,country,expat_status,language or 'All languages',gender,len(data_c_e_l_g))

                    vectors.append(interest_vector)

    return vectors



def accept_index(i, vector1, vector2):
    return (vector1[i] is not None) and (vector2[i] is not None)

def count_acceptable_rows(vector1, vector2):
    return len([i for i in range(len(vector1)) if accept_index(i, vector1, vector2)])

def cosine_similarity(vector1, vector2):
    def magnitude(vector):
        return numpy.sqrt(sum(vector[i]**2 for i in range(len(vector)) if accept_index(i, vector1, vector2)))

    if (len(vector1) != len(vector2)):
        return -1

    if (magnitude(vector1) == 0) or (magnitude(vector2) == 0):
        return -1

    return float(sum(vector1[i]*vector2[i] for i in range(len(vector1)) if accept_index(i, vector1, vector2))) /    \
        float(magnitude(vector1)*magnitude(vector2))

'''
def assimilation_score(vector1, vector2):
    target = vector1
    dest = vector2

    #assimilation score per interest
    scores = [target[i]/dest[i] if accept_index(i, target, dest) else None for i in range(len(vector1))]

    #sort by prevalence in destination country
    sorted_scores = [i[0] for i in sorted(zip(scores, dest), key=lambda x: x[1]) if i[0] is not None]

    #
    return sorted_scores[len(sorted_scores)/2]
'''

def compare(vector1, vector2):
    vectorname1, vectordata1 = vector1
    vectorname2, vectordata2 = vector2

    print(vectorname1)
    print(vectorname2)

    A = cosine_similarity(vectordata1, vectordata2)
    print(A)

vectors = get_interest_vectors(data)

mypairs_collection = []
for i in vectors:
    mypairs = []
    for j in vectors:
        #if i[0][3] != j[0][3]: continue #reject different gender demo pairs
        #if i[0][0] != j[0][0]: continue #reject different country demo pairs
        #print(pairs[-1])
        mypairs.append( (cosine_similarity(i[1],j[1]), count_acceptable_rows(i[1],j[1])) )
    mypairs_collection.append(mypairs)

with open("output_cosine_similarity", "w") as out_file:
    out_file.write(";"+";".join(str(i[0]) for i in vectors))
    for vec,pairs in zip(vectors,mypairs_collection):
        out_file.write(str(vec[0]) + ";" + ";".join(str(i[0]) if i[1]>600 else "N/A" for i in pairs)+"\n")

with open("output_acceptable_interests", "w") as out_file:
    out_file.write(";"+";".join(str(i[0]) for i in vectors))
    for vec,pairs in zip(vectors,mypairs_collection):
        out_file.write(str(vec[0]) + ";" + ";".join(str(i[1]) for i in pairs)+"\n")

