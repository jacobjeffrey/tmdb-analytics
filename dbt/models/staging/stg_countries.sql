-- models/staging/stg_countries.sql
SELECT
    NULLIF(UPPER(iso_3166_1), '')::char(2) as country_code,
    NULLIF(english_name, '')::text as english_name,
    NULLIF(native_name, '')::text as native_name
FROM
    {{ source('raw', 'countries') }}