SELECT 
    CAST(CAMIS AS INTEGER) AS bar_id,
    CAST(DBA AS STRING) AS bar_pub_name, 
    CAST(BORO AS STRING) AS borough, 
    CAST(BUILDING AS STRING) as building, 
    CAST(STREET AS STRING) as address, 
    CAST(ZIPCODE AS STRING) AS zip, 
    CAST(PHONE AS STRING) AS phone, 
    latitude, 
    longitude,
    CAST(score AS STRING) AS score,
    CAST(grade AS STRING) AS grade,
    PARSE_DATE('%m/%d/%Y', grade_date) AS grade_dt
FROM {{ source('staging','nyc_bar_crawl_data2') }}

WHERE DBA NOT IN ('')
AND (
    (LOWER(DBA) LIKE '% bar %' OR LOWER(DBA) LIKE '% pub %' OR LOWER(DBA) LIKE '% beer %' OR LOWER(DBA) LIKE '% ale %') 
    OR (LOWER(DBA) LIKE '% bar' OR LOWER(DBA) LIKE '% pub' OR LOWER(DBA) LIKE '% beer' OR LOWER(DBA) LIKE '% ale')
)
AND grade_date IS NOT NULL
AND grade_date NOT IN ('')

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

HAVING 
    latitude IS NOT NULL
AND longitude IS NOT NULL

ORDER BY 2,1
