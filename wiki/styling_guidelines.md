This page outlines the styling conventions and best practices for writing SQL and dbt code in our repository. These guidelines help ensure that our code is clean, consistent, and easy to maintain, fostering better collaboration and reducing the risk of errors.

## General Guidelines

### Clarity and Readability
- Write queries with readability in mind. Aim for clarity over brevity.
- Break long queries into smaller, logical steps. Use CTEs (Common Table Expressions) and subqueries to modularize your code.
   - Name CTEs clearly to describe what they are doing (e.g., `cleaned_data`, `aggregated_sales`, `filtered_orders`).
   - **DO NOT** use subqueries. Example:
      ```sql
      SELECT
        users.user_id
        , codes.referral_code
      FROM users
      INNER JOIN (
         SELECT
            users
            , referral_code
         FROM user_referral
      ) AS codes
      ```


### Use of Comments
   - Use comments to explain complex logic, business rules, or any non-obvious transformations.
   - Comment sections of code where transformations happen, especially when applying filters, joins, or aggregations.

### Whitespace and Formatting
- **All SQL keywords should be in uppercase.**: `SELECT`, `FROM`, `AS`, `GROUP BY`, etc.
- **Line Length**: Keep lines to a maximum of 120 characters.
- **New Lines**: List each field on a new line and indent. Example:
     ```sql
     SELECT
       id
       , name
       , created_at
     FROM customers
     ```
- **Commas**: Place commas at the beginning of each line in a list (leading commas)
- **Joins**: Always place the `ON` and `AND` conditions on new lines, indented for better readability.
     Example:
     ```sql
     SELECT
       customers.id
       , customers.name
       , orders.order_value
     FROM customers
     JOIN orders
       ON customers.id = orders.customer_id
       AND orders.status = 'completed';
     ```
#### SQLFluff
SQLFluff is a popular open-source SQL linter and formatter designed to enforce coding standards and improve the readability of SQL queries.

SQLFluff is set up as a tool for this repo to help enforce the guidelines mentioned above, ensuring consistent and readable SQL code. You can check the rules SQLFluff is enforcing [in this config file](https://github.com/getquip/dbt/blob/main/.sqlfluff).

**How to Use SQLFluff**

To use SQLFluff, you must manually run it in your development environment. Here’s a step-by-step guide on how to run it:
- To lint a specific SQL file, run the following command:
 ```bash
 sqlfluff lint path/to/your/query.sql
 ```
 This will check your SQL file for any style or syntax issues according to the default rules or your custom configuration.
- SQLFluff can automatically fix some of the identified issues. To do this, run the following command:
 ```bash
 sqlfluff fix path/to/your/query.sql
 ```
 This will apply formatting fixes such as adjusting indentation, reordering commas, or fixing SQL keyword casing.

### **Referencing Sources and References at the Top**
In dbt transformations, it's a best practice to always reference the source tables at the top of your SQL file. This improves clarity and organization in your transformation logic, making it clear where the raw data is coming from and providing an easy point of reference for others working with the same dataset.

Referencing sources at the top also helps with:
- **Easier maintenance**: When sources are defined upfront, it becomes easier to update or modify the data source later, since the references are clearly stated in one place.
- **Improved readability**: It is immediately obvious where the data is coming from, which helps anyone reading the code to understand the flow of data better.
Source Example:
     ```sql
     WITH 
     source AS (
         SELECT * FROM {{ source('recharge', 'charges') }}
     )
     ```
Reference Example:
   ```sql
    WITH

    credit_accounts AS (
	SELECT * FROM {{ ref("stg_recharge__credit_accounts") }}
    )

    , credit_adjustments AS (
	SELECT * FROM {{ ref("stg_recharge__credit_adjustments") }}
    )

    , charges AS (
	SELECT * FROM {{ ref("stg_recharge__charges") }}
    )

    -------------------------------------------------------
    ----------------- FINISH REFERENCES -------------------
    -------------------------------------------------------
   ```

**Use of CTEs (Common Table Expressions)**
   - Use CTEs to break down complex queries into manageable chunks.

### Alias Usage
Aliases should be **meaningful and descriptive**, avoiding letter aliases like `c` or `tpm`. This improves readability and makes queries easier to understand and maintain.  

**Column Aliases**
- Use aliases **only when necessary**, such as:  
  - When renaming a column.  
  - When applying a transformation that requires an explicit name assignment, e.g., `LOWER(name) AS name`.  
- Avoid unnecessary aliases for columns that retain their original names.  

**Table Aliases in Joins  **
- When a query includes **joins**, all columns in the `SELECT` statement **must have an alias**.  
- This ensures clarity in determining which table a column originates from.  

**Example:**  
```sql
SELECT 
    orders.order_id
    , customers.customer_name
    , LOWER(products.product_name) AS product_name
FROM orders
JOIN customers
  ON orders.customer_id = customers.customer_id
JOIN products
  ON orders.product_id = products.product_id;
```

