# Transformation Layers
Data transformation is a critical aspect of the [ETL](https://www.getdbt.com/blog/extract-transform-load) or [ELT](https://www.getdbt.com/blog/extract-load-transform) process, where the "T" represents the transformation phase. We follow **dbt best practices**, which entail dividing the transformation process into distinct layers. Learn more about this approach [here](https://www.getdbt.com/analytics-engineering/modular-data-modeling-technique).

## Context
**Our Goal: Build Reusable, Intuitive Datasets**
Data transformation automates data cleaning and metric calculations, ensuring that accurate, meaningful datasets are consistently generated in the data warehouse on a defined schedule. By automating key models, we can avoid redundant calculations in the BI layer, allowing datasets to be directly referenced in reports or dashboards, speeding up computation time.

### Why Transformation Layers?
Different transformation layers are essential for organizing and structuring the data processing pipeline in a scalable and efficient way. By separating the transformation process into distinct layers—**Stage, Intermediate, and Mart**—we can ensure that each phase is focused on specific tasks. The **Stage Layer** handles raw data ingestion and basic cleaning, ensuring data consistency. The **Intermediate Layer** applies more complex transformations, such as joins and business logic, making the data more useful and ready for analysis. Finally, the **Mart Layer** contains the final, business-ready datasets that are optimized for reporting and decision-making. This layered approach not only enhances maintainability and reusability but also improves performance by streamlining the transformation process and ensuring that only the necessary transformations are applied at each stage.


### Normalization and Denormalization
Normalization and Denormalization are two key approaches to structuring data in the transformation layers, each with its benefits and use cases.

**Normalization** is the process of organizing data to reduce redundancy and ensure data integrity. In the context of data transformation, normalization typically involves breaking down large tables into smaller, related tables, each focusing on a specific entity. By normalizing the data, we can:
- Eliminate data duplication: Reduces storage requirements and ensures that changes to an entity are made in one place.
- Ensure data consistency: Improves data integrity by reducing anomalies caused by inconsistent data updates.

In our pipeline, normalization occurs primarily in the **Intermediate Layer**, where we consolidate data into entities.

**Denormalization**, on the other hand, is the process of combining related tables into a single, larger table, with the aim of improving query performance, especially for analytical queries. This process involves introducing some level of redundancy into the data, which can make reporting and analysis more efficient in certain use cases. By denormalizing the data, we can:
- Improve query performance: With fewer joins needed in queries, reports and dashboards can run faster, which is especially useful when working with large datasets or performing real-time analytics.
- Simplify data access: Users can access all required data from a single table, making it easier to work with for reporting and analysis.
- Optimize for read-heavy operations: In data marts or final business-ready datasets (such as in the Mart Layer), denormalization ensures that reports run efficiently, as the data is pre-aggregated and structured for easy consumption.

Denormalization typically takes place in the **Mart Layer**, where data is designed to be directly consumed by BI tools and users. 
![image](https://github.com/user-attachments/assets/11494f88-cc95-4720-b9d9-6f0802b5abcf)

### What is a Data Mart?
A data mart is the final layer of the data transformation process, designed to provide business users with easy access to clean, business-ready datasets. It stores a subset of the larger organization's data, focusing on a specific business unit’s needs and delivering denormalized, pre-aggregated data optimized for reporting and analytics.

A data mart serves as the access point for a department's data, while the underlying database and transformation layers (Stage → Intermediate → Mart) manage and process that information. Tools like Looker then retrieve, format, and visualize this data for decision-making.

**Why is a Data Mart Important?**
Data marts offer several advantages for companies:

- **Efficient Data Retrieval**: Data marts allow for quicker access to department-specific information, eliminating the need to search through the entire data warehouse.
- **Streamlined Decision-Making**: By providing relevant subsets of data, a data mart allows teams to make decisions based on the same set of information.
- **Improved Data Governance**: Data marts grant more granular access control, enhancing security and enforcing data governance policies.
- **Flexible Data Management**: Data marts, being smaller and containing fewer tables than a data warehouse, can be more easily managed and modified without major changes to the database.

By following this layered transformation approach, the data mart becomes a high-performance, trusted source for decision-making across the organization. 🚀

# Stage Layer
The **Stage Layer** is the first step in our data transformation pipeline and plays a crucial role in ensuring that raw data is ingested and cleaned in a consistent format. This is where light transformations are applied, such as renaming columns, standardizing data types, and cleaning raw values. It is essential that we follow best practices when contributing to this layer to maintain the integrity and structure of the data.

### Organization of the Stage Layer

The **Stage Layer** is designed to handle raw data from source systems and prepare it for further transformation. It is organized in the following way:

```
└── stage/
    ├── shopify/  # data source (dataset name) from raw layer
    │   ├── schemas/  # folder to store YAML schema files
    │   │   ├── stg_shopify__customers.yml 
    │   │   ├── stg_shopify__orders.yml  
    │   ├── stg_shopify__customers.sql
    │   ├── stg_shopify__orders.sql  
    ├── quip_public/
    │   ├── schemas/  
    │   ├── stg_quip_public__orders.sql 
```

1. **Datasets/Folders**: Each data source is represented as a folder in the `stage` directory. These folders are named after the data source to keep things clear and organized.
   - Example: A data source from Shopify would be housed in a folder named `shopify/`.
   
2. **Models**: Inside each dataset folder, models are organized and named with the following convention:
   - **Prefix**: Models are prefixed with `stg_` followed by the name of the dataset. This helps easily distinguish them from other layers in the transformation pipeline.
   - **Naming Convention**: Models are named as closely as possible to the source table name, with minor adjustments (like pluralization) if needed.
   - Example: For a table called `shopify.customers`, the model would be named `stg_shopify__customers`.
   - Example: For a table called `shopify.product`, the model would be named `stg_shopify__products`.
   
3. **Transformations**: The transformations in this layer are light and focus on basic data wrangling tasks such as:
   - Renaming columns
   - Standardizing data types
   - Cleaning up raw values (e.g., trimming spaces, removing null values)

### Basic Clean up Guides

#### Renaming Fields
| **Data Type**   | **Cleaning Guide**                                              | **Example**                                             |
|-----------------|-----------------------------------------------------------------|---------------------------------------------------------|
| `timestamp`     | Rename timestamps to use `_timestamp` or `_at` suffix.          | `time` → `timestamp` <br> `delivered_time` → `delivered_at` <br> `last_login` → `last_login_at`|
| `boolean`       | Rename booleans to begin with `is_` or `has_`.                  | `active` → `is_active` <br> `enabled` → `is_enabled`     |
| `numeric`       | Ensure numeric columns are named clearly, e.g., use `amount`, `price`, `value` and reflect the context context. | `total` → `amount` <br> `price` → `price_per_unit`       |
| `string`        | Ensure that the field name is intuitive.         | `type` → `fee_type` <br> `name` → `vendor_name` |
| `date`          | Use consistent naming for date fields, e.g., `_date`.           | `dob` → `date_of_birth` <br> `date_joined` → `join_date`  |
| `id`            | Use `_id` suffix to indicate identifier fields.                | `user` → `user_id` <br> `customer` → `customer_id`       |
| `currency`      | Use clear naming for monetary fields.                           | `total` → `total_amount` <br> `value` → `transaction_value`|
| `email`         | Use clear naming for user contact information.                  | `email` → `user_email` <br> `phone` → `contact_number`   |

#### Deduplicating
1. Identify the Duplicate Criteria
* Exact duplicates: Rows where all column values are the same.
* Subset duplicates: Rows that have duplicate values in certain key columns, such as unique identifiers or timestamps.

Deduplication can happen based on one or more fields such as id, email, transaction_id, or timestamp. In this case, it's important to **generate a new surrogate key** to uniquely identify each record in the deduplicated dataset.
Example:
```sql
SELECT
  dbt_utils.surrogate_key(['sku', 'date']) AS sku_date_id
  , sku
  , date
  , MAX(cost) AS cost
FROM cogs
GROUP BY customer_id, order_date;

```

2. Choose a Deduplication Method
* Using SQL's DISTINCT: This method returns only unique rows.
```sql
SELECT DISTINCT * FROM source;
```
* Using `GROUP BY`: This method groups data by unique columns and can be used to choose aggregate functions for fields like timestamps or totals.

```sql
SELECT customer_id, MAX(order_date) AS latest_order
FROM source
GROUP BY customer_id;
```
* Using `QUALIFY`: This method assigns a unique number to each row within each partition of a dataset. Then, filters out duplicates based on row numbers.
```sql
SELECT
 order_id
 , updated_at
 , total_amount
FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
```



# Intermediate Layer
The intermediate layer consolidates and combines the staged data in a **normalized** form. It includes more complex transformations, such as table joins, metric calculations, and the application of business logic. This layer bridges the raw data and the final, business-ready datasets in the mart.

Key Features
1. Normalization:
The data in the Intermediate Layer is normalized to eliminate redundancy and ensure consistency. This involves splitting data into smaller, more manageable tables that each focus on specific entities (such as customers, products, or transactions).

2. Complex Data Transformation:
Complex transformations are performed in this layer, including table joins (combining data from multiple tables to create a unified record) and the application of business logic (such as categorization, data filtering, and condition-based transformations).
These transformations help refine the data for specific business use cases, ensuring that the data meets the required structure and logic needed for reporting and analysis.

### Organization of the Intermediate Layer
To maintain a structured and scalable transformation process, the Intermediate Layer is organized by **entities**. Each table in this layer represents a well-defined entity and follows a standardized naming convention to distinguish between fact and dimension tables.

```bash
│── models/
│   ├── intermediate/           
│   │   ├── orders/ # tables related to orders
│   │   │   ├── schemas # folder to store YAML files
│   │   │   ├── int_dim_orders.sql # attributes of the order
│   │   │   ├── int_fct_orders__revenue.sql # revenue metrics of an order
```

#### Naming Convention and Table Types
The tables in the Intermediate Layer follow a structured naming pattern:
- **int_fct_entity** – Represents a fact table, containing measurable events or transactions (e.g., orders, shipments, payments).
- **int_dim_entity** – Represents a dimension table, containing descriptive attributes about entities (e.g., customers, products, locations).
- **int_dim_entity__details** – A more granular dim table
- **int_fct_entity__details** – A more granular fact table

# Mart Layer
The Mart Layer consists of the final, business-ready datasets optimized for reporting, analytics, and dashboarding. This layer takes the normalized data from the Intermediate Layer and denormalizes it to create easy-to-use tables that facilitate efficient querying and analysis.

The key goal of this layer is to provide datasets that are intuitive, performant, and ready for business consumption without requiring analysts to perform complex joins or transformations.

### Key Differences Between Intermediate and Mart Layers

| Feature        | Intermediate Layer                         | Mart Layer                           |
|-------------- |-------------------------------- |-------------------------------- |
| **Data Structure** | Normalized | Denormalized |
| **Purpose** | Create metrics | Provide business-ready datasets |
| **Joins** | Complex | Minimal |
| **User** | Data engineers | Business analysts, BI tools |


### Organization of the Mart Layer
The Mart Layer is structured into data marts, each designed to serve a specific business function or department. Data marts ensure that stakeholders across different teams have access to relevant, pre-processed, and denormalized datasets, reducing the need for complex joins and transformations at query time.

Additionally, the Mart Layer includes Global Fact and Dimension Tables (`fact/` and `dim/`). These datasets contain all-encompassing facts and dimensions that span multiple data marts, providing a single source of truth across departments.

```bash
dbt_project/
│── models/
│   ├── mart/
│   │   ├── facts # global facts
│   │   ├── dims # global dims
│   │   ├── finance # data mart for the finance department
│   │   ├── operations # data mart for the operations department
```


