# vim: set noexpandtab:
import os
import sys
import pandas

from pysocialwatcher import watcherAPI
from time import time, localtime
from lib import PersistentSet
from string import ascii_lowercase

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')


def collect():

	def save():
		unvisited_set.save()
		visited_set.save()

	last_update = time()

	working_folder = 'CACHE_audience_size'
	
	input_dataframe_file = 'CACHE_interest_discovery/dataframe.csv'
	
	'''
	~~~ SEARCH PARAMETERS ~~~
	
	Parameters supported by the API:
	
	"interests",					# we get this from the list
	"behaviors",					# eg. expats
	"education_statuses",
	"family_statuses",
	"relationship_statuses",
	"locales",						# language
	"genders",						# 0, 1, 2
	"age_min",
	"age_max",
	"geo_locations"					# country
	'''

	def get_search_param(countries, interests):
	    return {'name': ",".join(countries) + "_interest_collection",
		    'geo_locations': [{'name':'countries', 'values': countries}],
		    'ages_ranges': [{'min':18, 'max':50}],
		    'genders': [0,1],
		    'interests': [{ 'and':[i],
				    'name':str(i)} for i in interests],
		    }

	countries = sys.argv[1].split(",")
		
	if not os.path.isdir(working_folder):
		os.makedirs(working_folder)

	if not os.path.isfile(input_dataframe_file):
		print("ERROR: Could not find input file \"dataframe.csv\"")
		return
		

	interests = []
	data = pandas.read_csv(input_dataframe_file)
	for line in data.iterrows():
		audience = line[1]['audience_size']
		fb_id = line[1]['id']
		# We only need the id
		if (audience >= 1000000):
			interests.append(str(fb_id))

	search_param = get_search_param(countries, interests)
	try:
		watcher.check_input_integrity(search_param)
		watcher.expand_input_if_requested(search_param)
	except Exception:
		print(Exception)
		exit()

	collection_df = watcher.build_collection_dataframe(search_param)
	results = watcher.perform_collection_data_on_facebook(collection_df)

if __name__ == '__main__':
	collect()

