# Installing
## Use pyenv for managing your python version
Make sure you are running Python 3.9 or above. In order to have a good experience with Python versions, we recommend you use Pyenv. There are may good introductions to pyenv, like [Intro to pyenv](https://realpython.com/intro-to-pyenv/).

``` 
> python -V # outputs the version of Python
Python 3.9.9
``` 
## Use a virtual environment
We recommend that you use a [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment) when installing and running the tools. The most common one is [venv](https://docs.python.org/3/library/venv.html) 
 
```   
python -m venv ./.venv     # Creates a virtual env in the current folder
source .venv/bin/activate  # Activates the venv    
```
## Install using pip: 
```
python -m pip install folio_migration_tools
```
Test the installation by showing the help pages 
```   
python -m folio_migration_tools -h
```    
