# Data Culture dbt coding styling

  - [General guidelines](#general-guidelines)
  - [Profiles](#profiles)
  - [Project structure](#project-structure)
  - [Model Configuration](#model-configuration)
  - [Macros](#macros)
  - [Environment and Schema Organization](#environment-and-schema-organization)
  - [Development Checklists](#development-checklists)
  - [dbt Resources](#dbt-resources)
  - [Credits](#credits)

<br>

## General guidelines

Study and follow the adopted [Data Culture SQL style guide](https://github.com/datacult/AE-coding-conventions/blob/main/dc-sql_style.md). 

Ensure `sqlfluff` is setup in the directory the dbt project will be setup.


Leverage the official dbt best [practices](https://docs.getdbt.com/guides/legacy/best-practices).



## Profiles

####  Profile names should only the name of the company they're for.

#### Sensitive information should be hidden as much as possible by leveraging environment variables



## Project structure

The desire to have a project layout is to help with the decision making process and build a standard across projects.<br>This allows for ease in setting up projects and can reduce decision fatigue.
In other to allow easy of navigating around the project, it's important the project structure reflect how the data flows, step-by-step, from a wide variety of source-conformed models into fewer, richer business-conformed models.

To that effect, Models should be organized into folders corresponding to their purpose.

For each dbt project, there should be four layers:


#### Sources 

  * This is the initial entry point for models, as they are bought in from external sources via a data loader (Fivetran)
  * Each source should have their own folder, which corresponds to the schema they are being imported from
    * E.g. Data from `google_analytics` and `shopify` will have a subdirectories `google_analytics` and `shopify` respectively under the **sources** directory

  A `<source_name>.yml` file defining the dbt sources should exist within each source-specific directory. That is `google_analytics.yml` and `shopify.yml`.


##### dos and don'ts with naming the Sources directory

- [x] Naming the subdirectories based on the source system is best practice. That is, naming the subdirectories from where they come from is helpful as they tend to share similar loading methods and properties between tables and can be operated on similarly. So data from the internal transactional database can be a system, data from Shopify in another model, etc.
- [x] It is **NOT** recommended to name subdirectories based on loaders as this can be too broad. So all things loaded with Fivetran from multiple sources should not all be in one subdirectory.
- [x] It is also **NOT** recommended to create subdirectories based on business groupings like “marketing”, “finance” and so on. This is because we want to create single sources of truth and overlapping and conflicting definitions can be hidden by this method.


#### Staging Layer

This is the layer that is connected to the source and low-level transformations are performed here. This is where the modular building blocks of our transformation layer live. <br>This is where we are refining the blocks that will later be built into more intricate and useful structures
Each model in this layer bears a one-to-one relationship with the source data table it represents. It has the same granularity. <br> Transformation that occur in this layer include: 
* Renaming
* Type Casting
* Basic computations/Conversions
* Categorizing (Case when statements)

Transformation that we DON’T do in this layer are;
* Joins
* Aggregations

The base layer directory may be introduced when two sources must be **joined** to create a usable staging model.

```
        source -----> base -----╷
                                |---> staging -----> intermediate -----╷
        source -----> base------╵                               |----> mart
                                                                |
        source ---------------------> staging -----> intermediate -----╵
```
When it comes with naming files & models in directories, consistency is key. The file names must be unique and correspond to the name of the model when selected and created in the warehouse. As a result, as much clear information should be in the file name. 
This includes the prefix for which layer the model exists in, important grouping information and whatever specific information about the entity or transformation in the model.

A good example of naming format for the files would be *base_`[source]`__`[entity]`s*.sql:

The double underscore between source system and the entity is to visually distinguish the separate parts in the case of a source name having multiple words. 
This adds clarity to google_analytics__campaigns and makes it easier to read than google_analytics_campaigns. The former lets us know that the source is google_analytics that has a campaigns entity while the latter can be read as the same or as a source called google with an analytics_campaign entity.

- [x] Files should **NOT** be named without specifying the source. This form of naming fuels will break down over time plus you lose the advantage of being able to figure out where the model is from without using the DAG.
	In naming the models, it should read as well as possible and that means, it is fine and even expected to use plurals. So **orders** table reads better than *order* table.

Models in this layer are either materialized as `views` or `ephemeral` since they are not final artifacts are are building blocks for later models.
Models in this layer should have a 1:1 relationship to the sources and are the only place where the [source](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources) macro should be used.

Other Considerations can be found [here](https://docs.getdbt.com/guides/best-practices/how-we-structure/2-staging#staging-other-considerations).


#### intermediate Layer

This is the layer where most of the transformation takes place. This is where we bring together the blocks in the base layer. 
These models are built with specific purposes on the way to the final data products

When it comes to naming files, it is important to use the `intermediate_[entity]s_[verb]s`.sql format.
The best guiding principle is to think about verbs (e.g. *pivoted*, *aggregated_to_user*, *joined*, *fanned_out_by_quanity*, *funnel_created*, etc.) 
in the intermediate layer. In our example project, we use an intermediate model to pivot payments out to the order grain, so we name our model *intermediate_payments_pivoted_to_orders*. It’s easy for anybody to quickly understand what’s happening in that model, even if they don’t know SQL. 
That clarity is worth the long file name. It’s important to note that we’ve dropped the double underscores at this layer.
In moving towards business conformed concepts, we no longer need to separate a system and an entity and simply reference the unified entity if possible. 
In cases where you need intermediate models to operate at the source system level (e.g. *int_shopify__orders_summed*, *int_core__orders_summed* which you would later union), you’d preserve the double underscores 

In this layer, it is important to ensure that any CTEs used are named to provide clarity to anyone reading the code. An example would be pivot_and_aggregate_payments_to_order_grain as it gives a clear idea of what happens within the CTE.


##### dos and don'ts

- [x] This layer is NOT exposed to end users.
- [x] The models can either be materialized as a view in a custom schema or ephemerally.
- [x] To simplify structure, rather than have 10 joins in the mart, we can have most of the joins done here. That way, we can have maximum of 4 joins in the mart.
- [x] It is highly recommended to move complex bits to their own models. This includes fan outs or collapsing of models.
- [x] If a particular model is being used across multiple models, it should be made a macro instead of remaining a model. This is to keep things DRY (Don’t Repeat Yourself)


#### Marts

They are stores of models that describe business entities and processes. They are often grouped by business unit: marketing, finance, product. Models that are shared across an entire business are grouped in a core directory.

Models in this layer are materialized as **tables** and when they take too long to query, when that takes too long, we configure as [incremental models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models). We only add complexity when necessary.
* We are to build wide and denormalised tables as the final output as much as possible.
* Unless simple joins, joins in a model here should NOT be more than 3. This is because we are not going for complexity in this layer. Any complexity should be moved to the intermediate layer.

Other considerations can be found [here](https://docs.getdbt.com/guides/best-practices/how-we-structure/4-marts#marts-other-considerations).


```
├── README.md
├── analysis
├── dbt_project.yml
├── macros
├── models
│   ├── marts
│   │   ├── finance
│   │   │   ├── payments.sql
│   │   │   ├── customers.sql
│   │   │   ├── schema.yml
│   │   ├── marketing
│   │   │   ├── page_hits.sql
│   │   │   ├── sessions.sql
│   │   │   ├── users.sql
│   │   │   ├── schema.yml
│   │   └── etc.
│   ├── intermediate
│   │   └── finance
│   │       ├── intermediate_finance__models.yml
│   │       └── intermediate_payments_pivoted_to_orders.sql
│   │       └── schema.yml
│   ├── staging
│   │   ├── google_analytics
│   │   │   ├── schema.yml
│   │   │   ├── stg_google_analytics__ga_campaigns.sql
│   │   │   └── stg_google_analytics__ga_orders.sql
│   │   │   └── base
│   │   │       ├── base_google_analytics.yml
│   │   │       ├── base_google_analytics__campaigns__US.sql
│   │   │       ├── base_google_analytics__campaigns__CA.sql
│   │   │       ├── base_google_analytics__campaigns__NG.sql
│   │   ├── shopify
│   │   │   ├── schema.yml
│   │   │   ├── stg_stripe__users.sql
│   │   │   ├── stg_stripe__payments.sql
│   │   │   └── stg_stripe__refunds.sql
|   ├── sources
│   │   │   └── google_analytics
│   │   │       ├── google_analytics.yml
│   │   │   └── shopify
│   │   │       ├── shopify.yml
│   │   └── etc.
│   └── utils
├── packages.yml
├── seeds
└── snapshots
```


## Model Configuration

* Follow the naming convention defined for each models in the different layer defined [here](#project-structure)
* Each Model should have the appropritate optimization and configuration attributes (like sort/dist keys for Redshift, partition/clustering for BigQuery etc.) should be specified in the model.
* If a particular configuration applies to all models in a directory, it should be specified in the dbt_project.yml file.

In-model configurations should be specified like this for proper formating:

```

{{
  config(
    materialized = 'table',
    sort = 'id',
    dist = 'id'
  )
}}

```
* Each model should have a primary key. If no unique field exist, leverage dbt [surrogate](https://github.com/dbt-labs/dbt-utils#surrogate_key-source) utils to generate one by concatenating different granularity of the data to create one. You can check this [resource](https://www.getdbt.com/blog/the-most-underutilized-function-in-sql/) & [this](https://docs.getdbt.com/blog/sql-surrogate-keys) to understand why surrogate/unique keys are important
* Every subdirectory should contain a schema.yml file, in which each model in the subdirectory is tested.
* At a minimum, `unique` and `not_null` tests should be applied to the primary key of each model.
* Properly separate the category of fields in the final CTEs for each model for easy understanding. And ensure all model have **METADATA** field such as SYSDATE (That corresponds to the timestamp of when the model was ingested into the Datawarehouse) and other useful metadata:


```
/* Good */
select
    /* Primary key */
    id

    /* Foreign keys and other fields */
    , customer_id
    , region_id

    /* Timestamps */
    , created_at

    /* metrics*/
    , revenue
    , total_sale

    /* metadata*/
    ,_one
    ,SYSDATE

from sales


/* Bad */
select
    id
    , created_at
    , customer_id
    , region_id
    , status
    , total_amount
from sales

```

* Materialize each models to their respective schemas as defined in the schema [section](#environment-and-schema-organization):
    * All Base and Staging Layer models materialized as Ephemeral or Views and should go in a separate staging schema. These models should not be exposed and used for reporting and analysis by downstream users
    * All Marts model should be materialized as a table for fast querying and analysis for reporting purposes and leverage the custom schema to properly organize these models.
* All models should be properly documented with `test` included. 
* Avoid reserved words as column names for fields in a model 
* Use the same field names across models where possible, e.g. a key to the customers table should be named customer_id rather than user_id.



## Macros

Leverage [dbt-utils](https://github.com/fishtown-analytics/dbt-utils) packages as much as possible to avoid reinventing the wheel, as this adds several macros that are commonly useful to any datawarehouse which aren't available right out of the box.

Important ones to take note of:

* date_spine - Create a date range similar to the `generate_series()` available in Postgres
* star - This macro pulls all the columns from a table excluding the columns listed in the except argument
  * Incredibly helpful when joining two tables with shared fields and/or when there are too many fields to list after the join.
  * E.g. `select {{ dbt_utils.star(from=ref('my_model'), except=["exclude_field_1", "exclude_field_2"]) }} from {{ref('my_model')}}`
* surrogate_key - Implements a cross-database way to generate a hashed surrogate key using the fields specified. Use this when generating keys on a dimension table.
* unique_where - Test if there is a unique constraint where something else is true
  * E.g. See if all the values in column `foo` are unique where `bar IS FALSE`
* unique_combination_of_columns - This can be used to test for uniques from a surrogate key created from various columns.



## Environment and Schema Organization


#### Environment

Create two(2) Databases for project setup; 
* **transform/staging** : Database to host all development workflows, materialized tables and views to be validated & QAed before pushing to production.  <br> For easy data retrieval and it can organized into different **schemas** based on dbt project layout as follows:
    * Staging : All other models that need to be validated and QAed by users before pushing to production
    * Marts : Only for Facts, Dimensions and aggregated reporting table to be exposed to BI layers.
    * Intermediate/intermediate :- Business intermediate and source data transformations that won't be queried by downstream users for reporting purposes.
    * Tests (Optional) 
 
* **analytics** : This database will contain production ready (cleaned, on a schedule, query-ready) dimension and fact tables. <br> For easy data retrieval and it can organized into different **schemas** based on dbt project layout as follows:
    * Same as above in **transform/staging** db
    * Snapshots (Optional) : If SCD need to be captured


#### Schema

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



## Development Checklists

#### Pre Development 

- [x] All business requirements and metrics properly documented
- [x] Data Model design (if necessary) required for the metrics and report developed and shared for feedbacks. Reference [Lucidchart](https://www.lucidchart.com/) or [Plantuml](https://plantuml.com/) for ERD diagrams



#### Development 

- [x] All staging and intermediate models aren't exposed to downstream users
- [x] Staging and analytics models are separated in different databases
- [x] The correct naming convention is adopted for all models in different layers
- [x] All models are well tested atleast a `not_null` and `unique` test
- [x] All models are well documented. Every model has at least a description that will explain the what? Why? How?. Some things that should be explained are for example granularity, known issues with the data if exist etc.
- [x] SQL scripts follows Data Culture adopted [SQL guide](https://github.com/datacult/AE-coding-conventions/blob/main/dc-sql_style.md)
- [x] All models are properly configured with the correct materialization for each layer


#### Post Development 

- [x] All codes are reviewed and follows the adopted DC [PR guide](https://github.com/datacult/AE-coding-conventions/blob/main/dc-pr_template_style.md)



## dbt Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- See the [source code](https://github.com/fishtown-analytics/dbt)
- Understanding what [YMAL is](https://rollout.io/blog/yaml-tutorial-everything-you-need-get-started/)



## Credits

These coding conventions were inspired in part by:
  - [dbt Labs' dbt coding conventions](https://github.com/fishtown-analytics/corp/blob/b5c6f55b9e7594e1a1e562edf2378b6dd78a1119/dbt_coding_conventions.md)
  - [GitLab's SQL style guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
  - [dbt Best practices](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview)
