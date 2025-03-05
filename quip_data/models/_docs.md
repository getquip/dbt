{% docs amount %}
    The monetary value associated with the record.  
{% enddocs %}

{% docs source_synced_at %}
    The timestamp indicating when the record was last successfully synced from the source system.
{% enddocs %}

{% docs is_source_deleted %}
    A boolean flag indicating whether the record has been marked as deleted in the source system.  
    - `TRUE` (or `1`): The record was deleted at the source.  
    - `FALSE` (or `0`): The record is still active in the source system.  
    This can be useful for soft deletions and historical tracking.  
{% enddocs %}

{% docs created_at %}
    The timestamp indicating when the record was initially created in the source system.  
{% enddocs %}

{% docs updated_at %}
    The timestamp indicating when the record was last updated in the source system.
{% enddocs %}

{% docs sku %}
    The Stock Keeping Unit (SKU) is a unique identifier for a specific product or item in the inventory.  
{% enddocs %}

{% docs component_master_category %}
    The category that defines the broad classification of a component within the system.  
    This field helps group components into predefined categories, making it easier to organize, search, and report on different types of components.  
    It is used for categorization purposes across various operational processes.  
{% enddocs %}

{% docs component_master_subcategory %}
    The subcategory that further classifies a component within a broader master category.  
    This field provides a finer level of classification, helping to differentiate components within the same category for better organization and reporting.  
    It allows for more granular tracking and management of components.  
{% enddocs %}

{% docs component_category %}
    The classification that groups components into a broad category based on their characteristics or function.
{% enddocs %}

{% docs component_color %}
    The color of the component.  
    This field helps identify the visual attributes of a component, which can be important for inventory management, product offerings, and customer preferences.  
{% enddocs %}

{% docs component_material %}
    The material used to manufacture the component.  
    This field helps classify components based on their composition, aiding in inventory tracking, sourcing, and product categorization.  
{% enddocs %}

{% docs component_consumer %}
    The target consumer group for the component.  
    This field identifies whether the component is intended for end-users, businesses, or other specific market segments.  
{% enddocs %}

{% docs component_edition %}
    The edition or variation of the component.  
    This field is used to differentiate different versions of the same component, such as limited editions or special releases.  
{% enddocs %}

{% docs component_version %}
    The version of the component.  
    This field identifies updates or revisions made to the component, helping to track changes over time and ensure compatibility with other products or systems.  
{% enddocs %}

{% docs component_has_revenue %}
    A boolean flag indicating whether the component generates revenue.  
    - `TRUE` (or `1`): The component is part of a revenue-generating product or service.  
    - `FALSE` (or `0`): The component does not generate revenue.  
{% enddocs %}

{% docs component_is_smart %}
    A boolean flag indicating whether the component is a smart product, meaning it has integrated technology or connectivity features.  
    - `TRUE` (or `1`): The component is a smart product.  
    - `FALSE` (or `0`): The component is not a smart product.  
{% enddocs %}

{% docs component_is_refillable_product %}
    A boolean flag indicating whether the component is refillable.  
    - `TRUE` (or `1`): The component is refillable.  
    - `FALSE` (or `0`): The component is not refillable.  
{% enddocs %}
