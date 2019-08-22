import base64
from .dbs.db import Db
import datetime, re

def encode(_str):
	return base64.b64encode(_str.encode('utf-8'))
	
def decode(_str):
	return base64.b64decode(_str.decode("utf-8"))

def getDbFactory():
	return Db

def get_date(format = '%Y-%m-%d %H:%M:%S'):
	return datetime.datetime.now().strftime(format)

def get_data_type(value):
	patterns = {
		"FLOAT": r"DOUBLE|DECIMAL|FLOAT",
		"DATE":  r"DATE",
		"DATETIME": r"DATETIME|TIMESTAMP",
		'BIGINT':  r"INTEGER|INT|TINYINT|SMALLINT|MEDIUMINT|BIGINT|BIT"
	}

	for pattern_key in patterns:
		if re.compile(patterns[pattern_key], re.IGNORECASE).search(value):
			return pattern_key
	
	return 'VARCHAR'