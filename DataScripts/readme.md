## Initial Setup

### 1 - Run the following in this directory
`pip install --user -r requirements.txt && python -m spacy download en_core_web_sm`

Alternatively (if you have docker & vscode), open this folder in the devcontainer.

### 2 - configure `.env` file
create a `.env` file in this directory with the follwing variables 
```
cfg_sas=<SAS Url to dev instance>
branch_env=<Suffix to add to warehouse, storage and other cloud resources>
run_state=
```

**cfg_sas**
This is a a url to a configuration file.

**run_state**
Json with paths to video batches. If you are running interactively, this is not needed.

**branch_env**
leave blank to use prod environment. If specified, will use seperate stores/warehouses etc.. when you need an isolated environment. This seperate environment is created using `recfluence create-env` (not documented yet).

### 3 - Run
Run `python app.py -h` for help


