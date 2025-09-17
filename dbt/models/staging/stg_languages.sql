-- models/staging/stg_languages.sql
SELECT
    NULLIF(lower(iso_639_1), '')::char(2) as language_code,
    NULLIF(english_name, '')::text as english_name,
    NULLIF(name, '')::text as native_name
FROM
    {{ source('raw', 'languages') }}