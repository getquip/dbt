## 🗂️ Managing Stale Models in dbt

### Purpose

To avoid unnecessary computation and ensure clarity in our DAG, we have a structured approach for managing **stale models**—models whose data is no longer actively refreshed but are still required by downstream logic.

---

### 🔁 What Happens When a Model Becomes Stale?

1. **Keep the Original Model Definition**
   The stale model remains in its original layer (`stage`, `intermediate`, or `mart`). This preserves its transformation logic for historical reference and traceability.

2. **Create a Source Declaration**
   A new file named `_stale_models.yml` is created in the same directory. This file defines the corresponding table as a dbt `source()` using the location where the stale data historically lives.

3. **Update Downstream Dependencies**
   All models that previously used `ref('stale_model')` are updated to use `source('dataset', 'stale_model')`.

4. **Exclude from Production Runs**
   By removing `ref()` usage, the stale model is no longer part of the dbt DAG and will not be built or refreshed in scheduled production jobs.

---

### 📁 Directory Structure

```
models/
├── stage/
│   └── littledata/
│       ├── _sources.yml                  # original source definitions
│       ├── _stale_models.yml             # stale models as source declarations
│       └── some_stale_model.sql          # preserved transformation logic
├── intermediate/
│   └── customer_data_platform/
│       ├── schemas/                      # YAML definitions (model-level)
│       ├── stale/
│       │   ├── some_stale_model.sql      # preserved logic
│       │   └── _stale_models.yml         # source declaration
│       ├── non_stale_model.sql
├── mart/

```

---

### ✅ Benefits

* **No runtime cost**: Stale models are excluded from builds and schedules.
* **Lineage clarity**: `source()` makes it explicit that the data is static.
* **Historical tracking**: Original SQL logic is preserved for auditing and rollback.
* **Clean DAG**: Helps maintain a tidy and reliable transformation graph.
