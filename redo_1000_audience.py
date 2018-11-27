# vim: set noexpandtab:
import os
import sys
import pandas
import numpy as np

from pysocialwatcher import watcherAPI
from pysocialwatcher.utils import load_dataframe_from_file
from time import time, localtime
from lib import PersistentSet
from string import ascii_lowercase

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')
watcher.config(save_every=1)

def collect():

	if len(sys.argv) > 1:
		input_dataframe_file = sys.argv[1]

		data = pandas.read_csv(input_dataframe_file)
		#https://stackoverflow.com/questions/21608228/conditional-replace-pandas
		mask = data.mau_audience <= 1000
		column = 'response'
		data.loc[mask, column] = np.NaN

		data.to_csv(input_dataframe_file+'_retry.csv')
		#restart previous collection
		collection_df = load_dataframe_from_file(input_dataframe_file+'_retry.csv')
	
		results = watcher.perform_collection_data_on_facebook(collection_df)

if __name__ == '__main__':
	collect()

