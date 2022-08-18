SELECT 
    bar_id,
    bar_pub_name, 
    borough, 
    building, 
    address, 
    zip, 
    phone, 
    latitude, 
    longitude,
    score,
    grade,
    grade_dt
FROM {{ ref('stg_nyc_bar_crawl') }}