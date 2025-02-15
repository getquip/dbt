# dbt
This dbt project is designed to transform raw data into actionable insights. By leveraging modular SQL and robust transformations, it ensures data consistency, quality, and readiness for analytics.

## Getting Started

Try running the following commands:
- dbt run
- dbt test

**TIP: Ensure You Are in the dbt Project Directory**

Before running any dbt commands, make sure you are in the root directory of the dbt project. This is where the `dbt_project.yml` file is located.  

You can navigate to the project directory using the terminal:  
```bash
cd /path/to/your/dbt-project
```

Being in the correct directory ensures that dbt can locate all required configuration files, models, and dependencies to execute commands like `dbt run` or `dbt test`.  

---  

## Transformation Layers  

We are utilizing dbt best practices, which means we separate the stages of the transformation process into their respective layers. Read more [here](https://www.getdbt.com/analytics-engineering/modular-data-modeling-technique).

1. **Stage Layer**  
   The stage layer is responsible for bringing raw source data into the dbt project. Here, we apply light transformations such as renaming columns, standardizing data types, and cleaning up raw values. This layer ensures that the data is in a consistent and usable format for further processing.  

2. **Intermediate Layer**  
   The intermediate layer consolidates and combines staged data. It includes more complex transformations, such as joining tables, calculating metrics, or applying business logic. This layer acts as a bridge between raw data and the final, business-ready data sets.  

3. **Mart Layer**  
   The mart layer contains the final transformed data, optimized for analysis and reporting. These models are structured to serve end-user needs, focusing on performance, readability, and ease of access. This layer often aligns with specific business domains or use cases.  

Read up on the naming conventions!
---

## Development Environment

### Target (`dev`)  
The `dev` target is configured to provide developers with isolated environments to build and test dbt models without affecting production data. This target appends `-dev` to BigQuery (BQ) project names, ensuring a clear separation between development and production environments.  

Example:  
- Production BQ project: `quip-dw-stage`  
- Development BQ project: `quip-dw-stage-dev`  

### Custom Schemas  
Each developer will use a custom schema following the convention:  
`developer_name_schema`  
This ensures that models are organized per developer, avoiding conflicts during collaborative development.  

Example:  
- Production BQ project: `shopify`  
- Development BQ project: `atea_shopify` or `dipali_shopify`

<developer_name> is configured in `profiles.yml` as `DBT_GCP_BQ_DATASET`.

[Instructions on how to set DBT_GCP_BQ_DATASET](https://github.com/getquip/dbt/tree/main?tab=readme-ov-file#instructions-on-how-to-set-dbt_gcp_bq_dataset)

### Alignment of Destination Databases with Transformation Layers  
The destination databases in BigQuery align with the transformation layers and their corresponding folder structures. This ensures a seamless transition from development to production.  
- **Stage Layer**: Models in the `stage` folder will land in the `quip-dw-stage` project, `-dev` if in development mode.
- **Intermediate Layer**: Models in the `intermediate` folder will land in the `quip-dw-intermediate` project, `-dev` if in development mode.
- **Mart Layer**: Models in the `mart` folder will land in the `quip-dw-mart` project, `-dev` if in development mode.


## Development Tips

- [dbt codegen](https://hub.getdbt.com/dbt-labs/codegen/latest/) package is helpful for generating sources, yml files, etc.
```zsh
# Recommended:

## Generate _sources yml
$ dbt --quiet run-operation generate_source --args '{"schema_name": "SCHEMA_NAME", "database_name": "SOURCE_DATABASE"}' > models/stage/SCHEMA_NAME/_sources.yml

## Generate stage yml
$ dbt --quiet run-operation generate_model_yaml --args '{"model_names": ["MODEL_NAME"]}' > models/TRANSFORMATION_LAYER/SCHEMA_NAME/schemas/MODEL_NAME.yml
```

## Naming Conventions
**Stage Layer** :
- Datasets/folders are named after the data source
- Models are named with a `stg_DATASET_NAME__` prefix
- Models are named as close as possible to the source table name. Small changes like pluralization are ok.
- Examples:
   - `shopify.customer` --> `stg_shopify__customers`
   - `quip_public.orders` --> `stg_shopify__customers`

2. **Intermediate Layer** 
- Datasets/folders are named after the reporting entity. Entities are what we report on, regardless of data source.
- Models are named with a `int_` prefix, followed by `dim__` or `fct__` based on [Kimball's dimensional modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) followed by the entity and optionally, a more granular entity.
- Examples:
   - `int_dim_orders` : a model with order level qualtitative attributes
   - `int_dim_orders__line_items`: a model with order's line item level qualitative attributes
   - `int_fct_orders__cogs`: a model with order level quantitative attributes, specifically cost of goods

3. **Mart Layer**  
- Standard Datasets are general, normalized tables
   - `dims_` entity dims at the widest detail
   - `facts` entity facts at the widest detail
- Reporting Datasets are denormalized tables for specific departments
   - `marketing`
   - `finance`
   - `quip` - organization wide use cases

## Cost Optimization
- make a model incremental when its >5GiBs to process
- use `insert_overwrite` when the data is not updating in place
### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


