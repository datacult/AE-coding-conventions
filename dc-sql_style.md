# Data Culture dbt coding styling

  - [General Setup](#general-setup)
  - [SQL Styling and rules](#sql-styling-and-rules)
  - [Credits](#credits)


## General Setup

To enforce rules to the adopted styling `sqlfluff` will be leveraged. Reference this doc for more details on how to [set-up](https://github.com/datacult/dbt-project-guidelist) 


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
      id    as account_id,
      name  as account_name,
      type  as account_type,
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
  select
      deleted as is_deleted,
      sla     as has_sla
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
      select
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
  select
      primary_table.column_1,
      primary_table.column_2
  from primary_table
  where primary_table.column_3 IN (
      select distinct specific_column 
      from other_table 
      where specific_column != 'foo')

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
  select 
      iff(column_1 = 'foo', column_2,column_3) as logic_switch,
      ...

  -- vs 

  -- Not Preferred
  select
      case
          when column_1 = 'foo' then column_2
          else column_3
      end as logic_switch,
      ...
```
- Prefer IFF to selecting a boolean statement:

```
  -- Preferred
  select 
      iff(amount < 10,TRUE,FALSE) AS is_less_than_ten,
      ...
  -- vs

  -- Not Preferred
  select 
      (amount < 10) as is_less_than_ten,
      ...

```
- Prefer simplifying repetitive CASE statements where possible:

```
  -- Preferred
  select
      case field_id
          when 1 then 'date'
          when 2 then 'integer'
          when 3 then 'currency'
          when 4 then 'boolean'
          when 5 then 'variant'
          when 6 then 'text'
      END AS field_type,
      ...

  -- vs 

  -- Not Preferred
  select 
      case
          when field_id = 1 then 'date'
          when field_id = 2 then 'integer'
          when field_id = 3 then 'currency'
          when field_id = 4 then 'boolean'
          when field_id = 5 then 'variant'
          when field_id = 6 then 'text'
      end as field_type,
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

