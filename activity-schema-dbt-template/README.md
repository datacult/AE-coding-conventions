# Activity Schema dbt Template

A production-ready dbt template for implementing activity schema data modeling with identity resolution and temporal join analysis (WIP).

## 🎯 What is Activity Schema?

Activity schema is an event-centric data modeling approach that:
- Treats every business event as a first-class entity
- Enables temporal relationship analysis between activities
- Handles identity resolution across anonymous and known user states
- Provides flexible, behavioral-driven insights

Refer to the documentation [here](https://www.notion.so/Activity-Schema-Docs-224fa8808c1b80e9982be516399656ce_) for more deepdive into the Activity Schema paradigm and its philosophical approach

This template provides a complete framework with pre-built macros, identity resolution logic, and temporal join patterns (WIP).

## 📋 Prerequisites

- dbt Core or dbt Cloud
- Snowflake, BigQuery, or Redshift data warehouse
- Basic understanding of dbt project structure
- Raw event data from your application/website

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/datacult/AE-coding-conventions.git
cd activity-schema-dbt-template
```

### 3. Configure Your Data Warehouse

Update `profiles.yml` with your warehouse credentials as required. 

## 📁 Project Structure

```
activity_schema_dbt_template/
├── models/
│   ├── staging/                          # 🔧 CUSTOMIZE: Your data sources
│   │   ├── _sources.yml                  # Define raw data sources
│   │   ├── stg_web_events.sql           # Clean web analytics data
│   │   ├── stg_user_data.sql            # Clean user/CRM data
│   │   └── stg_other_events.sql         # Other activity sources
│   ├── identity/                         # 📋 COPY AS-IS: Identity resolution
│   │   ├── identity__person_stream.sql   # 📝 Template + customize WHERE clause
│   │   ├── identity_change_detector.sql  # 📋 Copy exactly
│   │   └── identity_time_windows.sql     # 📋 Copy exactly
│   ├── intermediate/
│   │   └── activity_streams/             # 📝 TEMPLATES: Customize per activity
│   │       ├── person_stream_signed_up.sql
│   │       ├── person_stream_made_purchase.sql
│   │       ├── person_stream_started_session.sql
│   │       └── _activity_streams.yml     # Documentation/tests
│   └── marts/
│       ├── core/                         # 🔧 CUSTOMIZE: Business needs
│       │   ├── dim_customers.sql
│       │   └── fct_daily_activities.sql
│       └── analysis/                     # 📝 TEMPLATES: Business questions
│           ├── conversion_analysis.sql
│           ├── attribution_analysis.sql
│           └── churn_analysis.sql
├── macros/
│   ├── activity_schema/                  # 📋 COPY AS-IS: Core framework
│   │   ├── resolve_identity.sql
│   │   ├── complete_temporal_join.sql
│   │   └── enhanced_multi_temporal_analysis.sql
│   └── business_logic/                   # 🔧 CUSTOMIZE: Client-specific
│       └── custom_metrics.sql
├── tests/                                # Data quality tests
│   ├── activity_schema/
│   │   └── test_identity_coverage.sql
│   └── reconciliation/
│       └── test_revenue_reconciliation.sql
├── docs/
│   ├── IMPLEMENTATION_GUIDE.md           # Step-by-step setup
│   ├── TEMPORAL_JOINS_REFERENCE.md       # All 12 join types
│   └── DECISION_MATRIX.md                # Business question → Join type
├── dbt_project.yml
├── packages.yml
└── README.md
```

### 📂 File Legend

- 📋 **Copy As-Is**: Use these files exactly as provided (core framework)
- 📝 **Template + Customize**: Follow the template pattern, customize for your data
- 🔧 **Fully Customize**: Build these based on the client's business needs and other data exploration done and agreed activities defined as aligned with the Client leveraging the design methodological [here](https://www.notion.so/The-Activity-Schema-Data-Model-Design-228fa8808c1b80f78becf6fdd750bf8c)

## 🎓 Implementation Guide

Refer to the documentation [here](https://www.notion.so/The-Activity-Schema-Data-Model-Implementation-269fa8808c1b80e5a8f8c15a59910846) for more deepdive

### Step 1: Create all Staging Models and required Intermediate models

### Step 2: Set Up Identity Resolution

Identity resolution connects anonymous and known user activities.

**1. Define where users reveal their identity:**

Edit `models/identity/identity__person_stream.sql`:

```sql
-- Add your identity-revealing activities
select * 
from {{ ref('stg_payment_strip') }}
where customer is not null 
  anonymous_customer_id is not null 

union all
select * from {{ ref('stg_sign_up') }}
where customer is not null 
  anonymous_customer_id is not null 

union all
.....
-- Add other activities where users reveal identity
```

**2. The framework handles the rest automatically** via:
- `identity_change_detector.sql` (tracks identity changes)
- `identity_time_windows.sql` (creates identity resolution windows)

### Step 2: Create Activity Streams

Activity streams are standardized representations of business events.

**Template Pattern:**

**Activities that Require Identity Resolution**

```sql
-- models/intermediate/activity_streams/person_stream_[activity_name].sql
{{ config(materialized='table') }}

{% call resolve_identity() %}
select 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'timestamp']) }} as activity_id,
    user_id as anonymous_customer_id,
    customer,  -- Will be resolved by macro where necessary
    '[activity_name]' as activity,
    timestamp as ts,
    revenue_amount as revenue_impact,
    page_url as link,
    
    -- Store event attributes as JSON
    object_construct(
        'product_id', product_id,
        'category', category,
        'utm_source', utm_source
    ) as feature_json
    
from {{ ref('stg_web_events') }}
where event_type = '[your_event_type]'
{% endcall %}
```

**Create one for each key activity:**
- `person_stream_signed_up.sql`
- `person_stream_made_purchase.sql`
- `person_stream_started_session.sql`
- `person_stream_viewed_product.sql`
- etc.


**Activities that do not Require Identity Resolution**

```sql 

-- models/intermediate/activity_streams/person_stream_[activity_name].sql
{{ config(materialized='table') }}

select 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'timestamp']) }} as activity_id,
    user_id as anonymous_customer_id,
    customer,  
    activity,
    timestamp as ts,
    revenue_amount as revenue_impact,
    page_url as link,
    
    -- Store event attributes as JSON
    object_construct(
        'product_id', product_id,
        'category', category,
        'utm_source', utm_source
    ) as feature_json,
    
    row_number() over (
        partition by user_id 
        order by timestamp
    ) as activity_occurrence,
    
    lead(timestamp) over (
        partition by user_id 
        order by timestamp
    ) as activity_repeated_at
    
from {{ ref('stg_web_events') }} ---- REPLACE THE STAGING/INTERMEDIATE MODELS
where event_type = '[your_event_type]'
```

### Step 3: Build Analysis Models

Use temporal joins to answer business questions.

**Example: Conversion Analysis**

```sql
-- models/marts/analysis/conversion_analysis.sql

{{ temporal_join(
    cohort_activity='signed_up_v2',
    target_activity='used_model_v2', 
    join_type='first_after',
    cohort_filter='first'
) }}

```

```sql
{{ multi_temporal_analysis(
    primary_cohort_activity='started_session_v2', 
    join_configs=[
        {
            'name': 'signed_up',
            'target_activity': 'signed_up_v2',
            'join_type': 'first_after'
        },
        {
            'name': 'used_model',
            'target_activity': 'used_model_v2',
            'join_type': 'first_after'
        }
    ],
    cohort_filter='first'
) }}

```


### Step 4: Build Dimensional Models (Optional)

Create familiar dimensional models for BI tools where necessary ontop of the activity streams:

```sql
-- models/marts/core/dim_customers.sql
-- Build customer dimension from activity streams
```

## 📊 The 12 Temporal Join Types

Refer to the documentation on [Temporal Joins Basics](https://www.notion.so/Temporal-Joins-Basics-224fa8808c1b80da9b4ff347c728ad74) and [dbt Implementation](https://www.notion.so/dbt-Activity-Schema-Temporal-Joins-Framework-27dfa8808c1b807da7dfcba5fd986cec) for more deepdive 

| Join Type | Use Case | Example |
|-----------|----------|---------|
| `first_ever` | Very first occurrence | First product viewed |
| `last_ever` | Very last occurrence | Last login date |
| `aggregate_all_ever` | Lifetime totals | Total lifetime revenue |
| `first_before` | First action before event | First marketing touch before purchase |
| `last_before` | Last action before event | Last page before checkout |
| `aggregate_before` | All actions before event | Total sessions before signup |
| `first_after` | First action after event | First feature used after onboarding |
| `last_after` | Last action after event | Last activity before churning |
| `aggregate_after` | All actions after event | Total purchases after signup |
| `first_in_between` | First in session/period | First page in session |
| `last_in_between` | Last in session/period | Exit page in session |
| `aggregate_in_between` | All in session/period | Total pages per session |

See [TEMPORAL_JOINS_REFERENCE.md](docs/TEMPORAL_JOINS_REFERENCE.md) for detailed documentation.

## 🧪 Validation/Testing

Refer to this [documentation](https://www.notion.so/Guide-Activity-Schema-Validation-203fa8808c1b808381f0ce2e4a83f660#203fa8808c1b808381f0ce2e4a83f660) on the validation checklist

## 📚 Documentation


**Built with ❤️ for the DC analytics engineering team - Shout to David/Brittany helping out on this knowledge base**