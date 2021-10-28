# VS-Py and VMIS-SQL
This repository contains the implementations for VS-Py and VMIS-SQL

## VS-py 
VS-Py is a Python-based implementation of the original VS-kNN approach, based on the [reference code](https://github.com/rn5l/session-rec) from the original VSkNN paper. 

### Python computational model
Tested to work with python 3.7

#### Setup a virtual environment (only needed once)
```console
(base) computer:python_impl$ virtualenv venv
(base) computer:python_impl$ source venv/bin/activate
(venv) (base) computer:python_impl$ pip install -r requirements.txt
```

#### Execute the code in the virtual environment
```console
(base) computer:python_impl$ source venv/bin/activate
(venv) (base) computer:python_impl$ python main_python.py path/to/training_data_1m.txt path/to/test_data_1m.txt predictions.txt latencies.txt
```


## VMIS-SQL
VMIS-SQL is an implementation of VMIS-kNN in SQL, which leverages the embeddable analytical database engine [DuckDB](https://duckdb.org/)

#### Execute the code in the virtual environment
```console
(base) computer:python_impl$ source venv/bin/activate
(venv) (base) computer:python_impl$ python main_duckdb.py path/to/training_data_1m.txt path/to/test_data_1m.txt predictions.txt latencies.txt
```

