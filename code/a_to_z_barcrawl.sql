SELECT DBA AS bar_pub_name, BORO AS borough, BUILDING as building, STREET as address, ZIPCODE AS zip, PHONE as phone
FROM `wise-logic-354820.bar_crawl_data.nyc_bar_crawl_data` 
-- WHERE BORO IN ('Manhattan')
WHERE DBA NOT IN ('')
AND (
(LOWER(DBA) LIKE '% bar %' OR LOWER(DBA) LIKE '% pub %' OR LOWER(DBA) LIKE '% beer %' OR LOWER(DBA) LIKE '% ale %') 
OR (LOWER(DBA) LIKE '% bar' OR LOWER(DBA) LIKE '% pub' OR LOWER(DBA) LIKE '% beer' OR LOWER(DBA) LIKE '% ale')
)
GROUP BY 1,2,3,4,5,6
ORDER BY 2,1