# Installing
Make sure you are running Python 3.9 or above. 
We recommend that you use a [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment) when running the tools.
## 1. Using pip and venv
### 2.1. Create and activate a [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment)   
```   
python -m venv ./.venv     # Creates a virtual env in the current folder
source .venv/bin/activate  # Activates the venv    
```
### 2. Install using pip: 
```
python -m pip install folio_migration_tools
```
### 3. Test the installation by showing the help pages 
```   
python -m folio_migration_tools -h
```    

## 2. Using pipenv
### 1. Run
```   
pipenv install folio-migration-tools
```   
### 2. Test the installation by showing the help pages
```  
pipenv run python3 -m folio_migration_tools -h
```  

## 3. Using Poetry
TBA
