{{ config(
    partition_by={
      "field": "source_synced_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "destination_port_code",
        "origin_port_code", 
		"transportation_method",
        "shipment_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('wen_parker', 'shipment_details') }}
)

, cleaned AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key([
        'house_bill_number'
        , 'master_bill_number'
        , 'transportation_method'
        , 'vendor'
        , 'origin_port_code'
        , 'destination_port_code'
        , 'delivery_address'
    ]) }} AS shipment_id
        , house_bill_number
        , master_bill_number
        , PARSE_DATE('%Y%m%d' , created_at) AS created_on
        , TRIM(LOWER(transportation_method)) AS transportation_method
        , co_load_bill_of_lading
        , container_id
        , vendor
        , origin_port_code
        , destination_port_code
        , CAST(gross_weight AS FLOAT64) AS gross_weight_kg
        , CAST(chargeable_weight AS FLOAT64) AS chargeable_weight_kg
        , CAST(units AS INTEGER) AS units
        , CAST(cubic_meters AS FLOAT64) AS cubic_meters
        , CAST(cartons AS INTEGER) AS cartons
        , PARSE_DATE('%Y%m%d' , cargo_received_at) AS cargo_received_on
        , PARSE_DATE('%Y%m%d' , actual_time_of_departure_origin_at)
            AS actual_time_of_departure_origin_on
        , PARSE_DATE('%Y%m%d' , actual_arrival_airport_wetport_at)
            AS actual_arrival_airport_wetport_on
        , PARSE_DATE('%Y%m%d' , custom_release_at) AS custom_release_on
        , delivery_address
        , PARSE_DATE('%Y%m%d' , delivered_at) AS delivered_on
        , source_synced_at
        , source_file_name
    FROM source
)

SELECT * FROM cleaned
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY shipment_id
        ORDER BY source_synced_at DESC, source_file_name DESC
    ) = 1
