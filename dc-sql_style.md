# Data Culture dbt coding styling

  - [General Setup](#general-setup)
  - [SQL Styling and rules](#sql-styling-and-rules)
  - [Credits](#credits)


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

### Best Practices
 - No tabs should be used - only spaces. Your editor should be setup to convert tabs to spaces.

 - Wrap long lines of code, between 80 and 100, to a new line.
 
 - Do not use the `USING` command in joins because it produces inaccurate results in Snowflake.

 - Understand the difference between the following related statements and use appropriately:
   - `UNION ALL` and `UNION`
   - `LIKE` and `ILIKE`
   - `NOT` and `!` and `<>`
   - `DATE_PART()` and `DATE_TRUNC()`

 - Use the `AS` operator when aliasing a column or table.

 - Prefer `DATEDIFF` to inline additions `date_column + interval_column`. The function is more explicit and will work for a wider variety of date parts.

 - Prefer `!=` to `<>`. This is because `!=` is more common in other programming languages and reads like "not equal" which is how we're more likely to speak.

 - Prefer `LOWER(column) LIKE '%match%'` to column `ILIKE '%Match%'`. This lowers the chance of stray capital letters leading to an unexpected result.

 - Prefer `WHERE` to `HAVING` when either would suffice.

 - Maintain the same casing (UPPER or LOWER) across. We often prefer `Lower` 
 

### Commenting

  - When making single line comments in a model use the `--` syntax

  - When making multi-line comments in a model use the `/* */` syntax

  - Respect the character line limit when making comments. Move to a new line or to the model documentation if the comment is too long

  - Utilize the dbt model documentation when it is available

  - Calculations made in SQL should have a brief description of what's going on and if available, a link to the handbook defining the metric (and how it's calculated)

  - Instead of leaving TODO comments, create new issues for improvement

### Naming Conventions
 
  - An ambiguous field name such as `id`, `name`, or `type` should always be prefixed by what it is identifying or naming:

 ```
  -- Preferred
  select
      id    AS account_id,
      name  AS account_name,
      type  AS account_type,
      ...

  -- vs

  -- Not Preferred
  select
      id,
      name,
      type,
      ...

```
  - All field names should be [snake-cased:](https://en.wikipedia.org/wiki/Snake_case)

```
 -- Preferred
  select
      dvcecreatedtstamp AS device_created_timestamp
      ...

  -- vs

  -- Not Preferred
  select
      dvcecreatedtstamp AS DeviceCreatedTimestamp
      ...
```
 - Boolean field names should start with `has_`, `is_`, or `does_`:
```
 -- Preferred
  SELECT
      deleted AS is_deleted,
      sla     AS has_sla
      ...


  -- vs

  -- Not Preferred
  select
      deleted,
      sla,
      ...

```

  - Timestamps should end with `_at` and should always be in UTC.
  - Dates should end with `_date`.
  - Avoid key words like `date` or `month` as a column name.
  - When truncating dates name the column in accordance with the truncation.

```
select
      original_at,                                        -- 2020-01-15 12:15:00.00
      original_date,                                      -- 2020-01-15
      date_trunc('month',original_date) AS original_month -- 2020-01-01
      ...

```

### Reference Conventions
- When joining tables and referencing columns from both tables consider the following:
    - reference the full table name instead of an alias when the table name is shorter, maybe less than 20 characters. (try to rename the CTE if possible, and lastly consider aliasing to something descriptive)
    - always qualify each column in the SELECT statement with the table name / alias for easy navigation

```
-- Preferred
select
    budget_forecast_cogs_opex.account_id,
    date_details.fiscal_year,
    date_details.fiscal_quarter,
    date_details.fiscal_quarter_name,
    cost_category.cost_category_level_1,
    cost_category.cost_category_level_2
from budget_forecast_cogs_opex
left join date_details
    on date_details.first_day_of_month = budget_forecast_cogs_opex.accounting_period
left join cost_category
    on budget_forecast_cogs_opex.unique_account_name = cost_category.unique_account_name

 
-- vs 

-- Not Preferred
select
    a.account_id,
    b.fiscal_year,
    b.fiscal_quarter,
    b.fiscal_quarter_name,
    c.cost_category_level_1,
    c.cost_category_level_2
from budget_forecast_cogs_opex a
left join date_details b
    on b.first_day_of_month = a.accounting_period
left join cost_category c
    on b.unique_account_name = c.unique_account_name

```    
- Only use double quotes when necessary, such as columns that contain special characters or are case sensitive.

```
      -- Preferred
      select 
          "First_Name_&_" AS first_name,
          ...

      -- vs

      -- Not Preferred
      select 
          FIRST_NAME AS first_name,
          ...

```
- Prefer accessing JSON using the bracket syntax.

```
      -- Preferred
      select
          data_by_row['id']::bigint as id_value
          ...
        
      -- vs

      -- Not Preferred
      SELECT
          data_by_row:"id"::bigint as id_value
          ...

```
- Prefer explicit join statements.

```
      -- Preferred
      select *
      from first_table
      inner join second_table
      ...

      -- vs

      -- Not Preferred
      select *
      from first_table,
          second_table
      ...

```      
### Common Table Expressions (CTEs)
- Prefer CTEs over sub-queries as CTEs make SQL more readable and are more performant:

```
  -- Preferred
  with important_list AS (

      select distinct
          specific_column
      from other_table
      where specific_column != 'foo'
        
  )

  select
      primary_table.column_1,
      primary_table.column_2
  from primary_table
  inner join important_list
      on primary_table.column_3 = important_list.specific_column

  -- vs   

  -- Not Preferred
  SELECT
      primary_table.column_1,
      primary_table.column_2
  FROM primary_table
  WHERE primary_table.column_3 IN (
      SELECT DISTINCT specific_column 
      FROM other_table 
      WHERE specific_column != 'foo')

```
- Use CTEs to reference other tables.
- CTEs should be placed at the top of the query.
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as concise as possible while still being clear.
   - Avoid long names like replace_sfdc_account_id_with_master_record_id and prefer a shorter name with a comment in the CTE. This will help avoid table aliasing in joins.
- CTEs with confusing or notable logic should be commented in file and documented in dbt docs.
- CTEs that are duplicated across models should be pulled out into their own models.
### Data Types
- Use default data types and not aliases. Review the Snowflake summary of data types for more details. The defaults are:
   - NUMBER instead of DECIMAL, NUMERIC, INTEGER, BIGINT, etc.
   - FLOAT instead of DOUBLE, REAL, etc.
   - VARCHAR instead of STRING, TEXT, etc.
   - TIMESTAMP instead of DATETIME
The exception to this is for timestamps. Prefer TIMESTAMP to TIME. Note that the default for TIMESTAMP is TIMESTAMP_NTZ which does not include a time zone.

### Functions
- Prefer IFNULL to NVL.
- Prefer IFF to a single line CASE statement:

```
  -- Preferred
  SELECT 
      IFF(column_1 = 'foo', column_2,column_3) AS logic_switch,
      ...

  -- vs 

  -- Not Preferred
  SELECT
      CASE
          WHEN column_1 = 'foo' THEN column_2
          ELSE column_3
      END AS logic_switch,
      ...
```
- Prefer IFF to selecting a boolean statement:

```
  -- Preferred
  SELECT 
      IFF(amount < 10,TRUE,FALSE) AS is_less_than_ten,
      ...
  -- vs

  -- Not Preferred
  SELECT 
      (amount < 10) AS is_less_than_ten,
      ...

```
- Prefer simplifying repetitive CASE statements where possible:

```
  -- Preferred
  SELECT
      CASE field_id
          WHEN 1 THEN 'date'
          WHEN 2 THEN 'integer'
          WHEN 3 THEN 'currency'
          WHEN 4 THEN 'boolean'
          WHEN 5 THEN 'variant'
          WHEN 6 THEN 'text'
      END AS field_type,
      ...

  -- vs 

  -- Not Preferred
  SELECT 
      CASE
          WHEN field_id = 1 THEN 'date'
          WHEN field_id = 2 THEN 'integer'
          WHEN field_id = 3 THEN 'currency'
          WHEN field_id = 4 THEN 'boolean'
          WHEN field_id = 5 THEN 'variant'
          WHEN field_id = 6 THEN 'text'
      END AS field_type,
      ...
```    
- Prefer the explicit date function over date_part, but prefer date_part over extract:
```
  DAYOFWEEK(created_at) > DATE_PART(dayofweek, 'created_at') > EXTRACT(dow FROM created_at)
```
- Be mindful of date part interval when using the DATEDIFF function as the function will only return whole interval results.


  ## Other SQL Style Guide
There are other style guides one could use:
- [Brooklyn Data Co](https://github.com/brooklyn-data/co/blob/main/sql_style_guide.md)
- [Fishtown Analytics](https://github.com/dbt-labs/corp/blob/main/dbt_coding_conventions.md#sql-style-guide)
- [Matt Mazur](https://github.com/mattm/sql-style-guide)
- [Kickstarter](https://gist.github.com/fredbenenson/7bb92718e19138c20591)

