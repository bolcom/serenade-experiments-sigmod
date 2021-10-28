# Session Based Recommendations
This repository contains code for session-based recommender system Serenade.
It learns users' preferences by capturing the short-term and sequential patterns from the evolution of user 
behaviors and predicts interesting next items with low latency. 

# Quick guide: getting started with Serenade.

## Table of contents
1. [Find the best hyperparameter values](#find-hyperparams)
2. [Configure Serenade to use the hyperparameter values](#update-config)
3. [Start the Serenade service](#start-service)
4. [Retrieve recommendations using python](#retrieve-recommendations)
5. [Research experiments](#research-experiments)

This guide assumes you have the following:
- Binary executables for your platform. (mac, linux and windows are supported)
```
serving
hyperparameter_search
```
- A configuration file 'Default.toml'
- A csv file with training data 'retailrocket9_train.txt' in /datasets/
- A csv file with test data 'retailrocket9_test.txt' in /datasets/

### Find the best hyperparameter values <a name="find-hyperparams"></a>
Execute hyperparameter_search for your platform with the location of the train and test file.
```bash
./hyperparameter_search /datasets/retailrocket9_train.txt /datasets/retailrocket9_test.txt
```
After a few minutes you should see a message that it has found the best hyperparameters for the best Mean Reciprocal Rank at 20 (MRR@20) of 0.1630. (The actual values might differ)
```
Best hyperparameter values found:,{"neighborhood_size_k": 1000, "max_items_in_session": 7, "m_most_recent_sessions": 250} with Mrr@20:0.16304121849431474
```

### Configure Serenade to use the hyperparameter values <a name="update-config"></a>
We now update the configuration file 'Default.toml' to use the hyperparameter values and set the training_data_path with the location of the retailrocket9_train.txt.
This is an example of the full configuration file
```
config_type = "toml"

[server]
host = "0.0.0.0"
port = 8080
num_workers = 4

[log]
level = "info" # not implemented

[data]
training_data_path=/datasets/retailrocket9_train.txt

[model]
neighborhood_size_k = 1000
max_items_in_session = 7 
m_most_recent_sessions = 250
num_items_to_recommend = 21

[logic]
enable_business_logic = "false"
```


### Start the Serenade service <a name="start-service"></a>
Start the 'serving' binary for your platform with the location of the configuration file 'Default.toml' as argument
```bash
./serving Default.toml
```

You can open your webbrowser and goto http://localhost:8080/ you should see an internal page of Serenade.


### Retrieve recommendations using python <a name="retrieve-recommendations"></a>

```python
import requests
from requests.exceptions import HTTPError
try:
    myurl = 'http://localhost:8080/v1/recommend'
    params = dict(
        session_id='144',
        user_consent='true',
        item_id='453279',
    )
    response = requests.get(url=myurl, params=params)
    response.raise_for_status()
    # access json content
    jsonResponse = response.json()
    print(jsonResponse)
except HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except Exception as err:
    print(f'Other error occurred: {err}')
```
```
[72916, 84895, 92210, 176166, 379693, 129343, 321706, 257070]
```
The returned json object is a list with recommended items.


## Research experiments <a name="research-experiments"></a>

The data for creating Figure 3 (a) "Offline and online performance of Serenade" in the paper was created using 
```bash
cargo build --release
target/release/paper_micro_benchmark_runtimes
```

The data for creating Figure 2 "Sensitivity of MRR@20 and Prec@20 to the hyperparameters..." in the paper was created using
```bash
cargo build --release
target/release/paper_hyperparam_sensitivity 
```
