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

def get_global_interest_dict_id_to_name(filename):

    global_interests = pandas.read_csv(GLOBAL_DATAFRAME_FOLDER + '/dataframe_interest_names.csv')

    interest_id_to_name = {}
    global_mau = {}

    for index, row in global_interests.iterrows():
        interest_id_to_name[row['id']] = row['name']

    return interest_id_to_name

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

def get_interest_vectors(data):

    vectors = []
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

                    data_c_e_l_g = data_c_e_l[data_c_e_l['gender_id'] == gender]
                    interest_vector = [(country, expat_status, language or 'All', gender),
                                            data_c_e_l]
                    print('\t'*2,country,expat_status,language or 'All languages',gender,len(data_c_e_l_g))

    return vectors

vectors = get_interest_vectors(data)

