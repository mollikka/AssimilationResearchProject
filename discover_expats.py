# vim: set noexpandtab:
import os
import sys
import pandas

from pysocialwatcher import watcherAPI
from pysocialwatcher.utils import load_dataframe_from_file
from time import time, localtime
from lib import PersistentSet
from string import ascii_lowercase

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')
watcher.config(save_every=1)

EXPAT = 6015559470583

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

	expat_koodi = {
		'BD' : 6023356562783,	# Bangladesh
		'SN' : 6023357000583,	# Senegal
		'CD' : 6023516373983,	# Congo
		'NG' : 6018797004183,	# Nigeria
		'LK' : 6023516315983,	# Sri Lanka
		'RS' : 6027149004983	# Serbia
	}

#	python skript.py IT BD,SN

	def get_search_param(countries, interests, expats):
	    return {'name': countries + "_interest_collection",
		    'geo_locations': [{'name':'countries', 'values': [countries]}],
		    'ages_ranges': [{'min':18, 'max':65}],
		    'genders': [0],
		    'behavior': #{'or': [EXPAT], 'name': 'Ex-pat'},
				[{'or': [expat_koodi[maakoodi]], 'name': maakoodi} for maakoodi in expats],
				 #{'not': [EXPAT], 'name': 'Not ex-pat'}],
		    'interests': [{ 'and':[i],
				    'name':[str(i)]} for i in interests],
		    }

	# [{'or': [expat_koodi[maakoodi]], 'name': maakoodi} for maakoodi in arglist]

	countries = sys.argv[1]
	expats = sys.argv[2].split(",")
		
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

	search_param = get_search_param(countries, interests, expats)
	try:
		watcher.check_input_integrity(search_param)
		watcher.expand_input_if_requested(search_param)
	except Exception:
		print(Exception)
		exit()

	if len(sys.argv) == 2:
		#restart previous collection
		collection_df = load_dataframe_from_file(sys.argv[1])	
	else:
		#start from scratch
		collection_df = watcher.build_collection_dataframe(search_param)
	
	results = watcher.perform_collection_data_on_facebook(collection_df)

if __name__ == '__main__':
	collect()

