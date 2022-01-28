select timestamp as Timestamp, EXTRACT(HOUR FROM timestamp) AS Hour,
          EXTRACT(MINUTE FROM timestamp) AS Minute,
          EXTRACT(SECOND FROM timestamp) AS Second, 
          EXTRACT(YEAR FROM timestamp) AS year, 
          DATE(timestamp) as Date,
          category as class,
          col_count as Count,
          location  as Branch          
           FROM {{ source('S3','timhortons_new') }}
