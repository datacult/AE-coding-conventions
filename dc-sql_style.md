# Data Culture dbt coding styling

  - [General Setup](#general-setup)
  - [SQL Styling and rules](#sql-styling-and-rules)
  - [Other SQL Style Guide](#other-sql-style-guide)


## General Setup

To enforce rules to the adopted styling `sqlfluff` will be leveraged. 

### Setup 

#### Poetry

Dependency management and packaging is done with [poetry](https://python-poetry.org/). You will need it to install the virtual environment `dbt` runs in as well as other packages required to contribute. Poetry operates much like [venv](https://docs.python.org/3/library/venv.html) where the environment needs to be activated before it can be used.

1. Download and install [python](https://www.python.org/downloads/)

2. Install pyenv following the following process
```
brew update

brew install pyenv

```

* Configure your Mac's environment

```
echo 'eval "$(pyenv init -)"' >> ~/.bash_profile

```

* Activate your changes 

```
source ~/.bash_profile

```

* You can use pyenv to install any version required for development based on your choice

e.g. 
- run `pyenv install 3.5.0` in your terminal install python 3.5.0
- Check the version of python running in your local
    * run pyenv versions

  
3. Install poetry

```
`$ curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -`

```

Verify the installation. You should see a version number pop up.

```
`$ poetry --version`

```

4. cd to the project directory created as instructed in the setup [guide](https://www.notion.so/dbt-Setup-guide-5f8820554ea948f3a10cda2a3d2cf7c9) either the cloned repo or the initially setup directory

5. Once inside the directory, set the local Python version you are going to use. This will prompt poetry to use the local version of Python defined by pyenv:
* run pyenv versions and you can set any of the returned version 

e.g. 

To set the local directory to run on python 3.7.0:

execute `pyenv local 3.7.0`  and this prompt poetry to use the local version set. 

6. Create a `pyproject.toml` file inside the same directory and paste the following :


```
[tool.poetry]
name = "dbt"
version = "0.1.0"
description = ""
authors = ["Your Name <you@example.com>"]

[tool.poetry.dependencies]
python = "^3.9.11"
dbt-core = "1.0.6"
dbt-redshift = "1.0.1"
dbt-snowflake = "1.0.1"
dbt-snowflake = "1.0.1"
sqlfluff = "1.2.1"
sqlfluff-templater-dbt = "1.2.1"

[tool.poetry.dev-dependencies]
pytest = "^5.2"

[tool.sqlfluff.core]
templater = "jinja"
dialect = "redshift"

[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"


```

Replace the necessary details in the file with information that reflect the environment you are developing on such as : 
* The python version to the version you set in `step 5` 
* dialect to the datawarehouse been used that is snowflake, bigquery etc.


7. run `poetry install` to allow poetry automatically instally dbt as well as all the necessary dependencies required for sqlfluff to run

8. run `poetry shell` to activate the the new environment created and manage all dependecies

9. To confirm `sqlfluff` and `dbt` is fully installed and all necessary connections setup in `profile.yml` file is correct run the following command:

```
sqlfluff version

dbt --version

dbt --version

dbt debug

```

10. To confirm `sqlfluff` is fully activated create a `test.sql` model with the code:

```

SELECT a+b  AS foo,
c AS bar from my_table

```

* Save the file

* run `sqlfluff lint test.sql` to see the result returned

The result below should be outputed 

```

== [test.sql] FAIL                                                                                                                                              
L:   1 | P:   1 | L034 | Select wildcards then simple targets before calculations
                       | and aggregates.
L:   1 | P:   1 | L036 | Select targets should be on a new line unless there is
                       | only one select target.
L:   1 | P:   9 | L006 | Missing whitespace before +
L:   1 | P:   9 | L006 | Missing whitespace after +
L:   1 | P:  11 | L039 | Unnecessary whitespace found.
L:   2 | P:   1 | L003 | Expected 1 indentations, found 0 [compared to line 01]
L:   2 | P:  10 | L010 | Keywords must be consistently upper case.
L:   2 | P:  26 | L009 | Files must end with a single trailing newline.


```

To allow `sqlfluff` correct the formatting after checking the error message and manually correcting when necessary:

executre:

```
 
sqlfluff fix test.sql

save the test.sql file again

sqlfluff lint test.sql` 

```

## SQL Styling and Rules

### General Guidelines

  - It is important that code is [DRY](https://docs.getdbt.com/terms/dry). Utilise CTES, jinja and macros in dbt. Remember that if you type the same line twice, it needs to be maintained in two places.
  - Do not optimmize for fewer lines of code, new lines are cheap but brain time is expensive.
  - Be consistent. Even if you are not sure of the best way to do something do it the same way throughout your code, it will be easier to read and make changes if they are needed.
  - Be explicit. Defining something explicitly will ensure that it works the way you expect and it is easier for the next person, which may be you, when you are explicit in SQL.

## Other SQL Style Guide

