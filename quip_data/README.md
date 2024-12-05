Welcome to your new dbt project!

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

1. **Stage Layer**  
   The stage layer is responsible for bringing raw source data into the dbt project. Here, we apply light transformations such as renaming columns, standardizing data types, and cleaning up raw values. This layer ensures that the data is in a consistent and usable format for further processing.  

2. **Intermediate Layer**  
   The intermediate layer consolidates and combines staged data. It includes more complex transformations, such as joining tables, calculating metrics, or applying business logic. This layer acts as a bridge between raw data and the final, business-ready data sets.  

3. **Mart Layer**  
   The mart layer contains the final transformed data, optimized for analysis and reporting. These models are structured to serve end-user needs, focusing on performance, readability, and ease of access. This layer often aligns with specific business domains or use cases.  

---

## Development

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

[Instructions on how to set DBT_GCP_BQ_DATASET](https://github.com/getquip/dbt/tree/main/Instructions-on-how-to-set-DBT_GCP_BQ_DATASET)

### Alignment of Destination Databases with Transformation Layers  
The destination databases in BigQuery align with the transformation layers and their corresponding folder structures:  
- **Stage Layer**: Data is written to schemas or datasets prefixed with `stg_`.  
- **Intermediate Layer**: Data is written to schemas or datasets prefixed with `int_`.  

This structure mirrors the folder organization in the dbt project and ensures a seamless transition from development to production.


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
