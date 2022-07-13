# Data Culture dbt coding styling

  - [General guidelines](#general-guidelines)
  - [Project structure](#project-structure)
  - [Model Configuration](#model-configuration)
  - [Naming Conventions](#naming-conventions)
  - [Packages](#packages)
  - [dbt Sources](#dbt-sources)
  - [Models](#models)
  - [Seeds](#seeds)
  - [Macros](#macros)
  - [YAML](#yaml)
  - [Environment & Schema Organization](#environment-&-schema)
  - [Credits](#credits)

<br>

## General guidelines


## Project structure


## Model Configuration

## Naming Convention 

## Packages


## Models


## Seeds


## Macros

## YAML


## Environment & Schema Organization

### Environment

Create two(2) Databases for project setup; 
* transform/staging : Database to host all development workflows, materialized tables and views to be validated & QAed before pushing to production.  <br> For easy data retrieval and it can organized into different **schemas** based on dbt project layout as follows:
    * Staging : All other models that need to be validated and QAed by users before pushing to production
    * Marts : Only for Facts, Dimensions and aggregated reporting table to be exposed to BI layers.
    * Intermediate/Logic :- Business Logic and source data transformations that won't be queried by downstream users for reporting purposes.
    * Tests (Optional) 
 
* analytics : This database will contain production ready (cleaned, on a schedule, query-ready) dimension and fact tables. <br> For easy data retrieval and it can organized into different **schemas** based on dbt project layout as follows:
    * Same as above in **transform/staging** db
    * Snapshots (Optional) : If SCD need to be captured

### Schema

#### [Check out dbt officials docs on the behavior of default schema](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas)

- In production and development database environment, appropriate custom schemas should be utilized to define which schemas each model or view should be materialized in.

Use the `generate_schema_name` macro below to dynamically determine where to materialize the models:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'dev' or target.name=='default' -%}
        {{ target.schema }}{{ '_' ~ custom_schema_name if custom_schema_name else '' }}

    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name if custom_schema_name else target.schema }}

    {%- endif -%}

{%- endmacro %}

```
*The custom `generate_schema_name` macro can be customized to fit purpose*


## Credits

These coding conventions were inspired in part by:
  - [dbt Labs' dbt coding conventions](https://github.com/fishtown-analytics/corp/blob/b5c6f55b9e7594e1a1e562edf2378b6dd78a1119/dbt_coding_conventions.md)
  - [GitLab's SQL style guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
