import dask
import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client
import joblib

from dask_ml.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.dummy import DummyClassifier

print("I do something now :)")
# InterProScan default column names
COLUMNS = ['prot_access', 'sequ_md5_dig', 'seq_len',
'analysis', 'sign_access', 'sign_descr', 'start_loc', 'stop_loc',
'score', 'status', 'date', 'iPro_access', 'iPro_descr',
'GO_annot', 'pathway']

PATH = '/homes/mjalali/Documents/programming3/Assingment6/data/mock_data_100000.csv'
# PATH = '/data/dataprocessing/interproscan/all_bacilli.tsv'

def train(model_name, X_train, y_train):
	models = {
		'rfc': RandomForestClassifier(random_state=0),
		'ada': AdaBoostClassifier(random_state=0),
		'lr': LogisticRegression(random_state=0),
		'dummy': DummyClassifier(strategy="most_frequent")
	}

	model = models[model_name]
	with joblib.parallel_backend('dask'):
		model.fit(X_train, y_train)

	return model

if __name__ == '__main__':
	df = dd.read_csv(PATH, dtype={'score':'object'}, blocksize='500MB').set_index('prot_access')
	# df = dd.read_csv(PATH, sep='\t', names=COLUMNS, dtype={'score': 'object'}, blocksize='500MB').set_index('prot_access')
	print("I read in the file at least....")
	selection = df[df['iPro_access'] != '-']
	selection = selection[['seq_len', 'iPro_access', 'start_loc', 'stop_loc']]
	selection['difference'] = (selection['stop_loc'].astype(float) - selection['start_loc'].astype(float)) / selection['seq_len'].astype(float)

	computed = selection[['iPro_access', 'difference']]

	test = computed.reset_index()
	test = test.categorize(columns=['iPro_access'])
	print(" pivoting the table now :)")
	piv = dask.dataframe.reshape.pivot_table(test, index='prot_access', columns='iPro_access')

	y = piv.idxmax(axis=1)
	t = y.compute()
	_, labels = t.str

	X = piv.notnull().astype(int)

	enc = LabelEncoder()
	enc.fit(labels)
	y_labels = enc.transform(labels)
	print("To dask array now! :)")
	X_arr = X.to_dask_array(lengths=True)
	y_arr = da.from_array(y_labels, chunks=X_arr.chunksize[0])

	X_train, X_test, y_train, y_test = train_test_split(X_arr, y_arr, test_size=0.2, random_state=0)

	client = Client(processes=False)
	print("Traininng the modeloeskiesjooa")
	rfc = train('rfc', X_train, y_train)
	ada = train('ada', X_train, y_train)
	lr = train('lr', X_train, y_train)
	dum = train('dummy', X_train, y_train)

	rfc_score = rfc.score(X_test, y_test)
	ada_score = ada.score(X_test, y_test)
	lr_score = lr.score(X_test, y_test)
	dum_score = dum.score(X_test, y_test)

	print('rfc: {:.4f}'.format(rfc_score))
	print('ada: {:.4f}'.format(ada_score))
	print('lr: {:.4f}'.format(lr_score))
	print('dummy: {:.4f}'.format(dum_score))


	print('...finished')