-- garf 1

SELECT 
    t.genre AS genre,
    SUM(fs.unit_price * fs.quantity) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_track t ON fs.track_id = t.dim_trackid
GROUP BY 
    t.genre
ORDER BY 
    total_revenue DESC;


-- graf 2 
SELECT 
    c.country AS country,
    SUM(fs.unit_price * fs.quantity) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_customer c ON fs.customer_id = c.dim_customerid
GROUP BY 
    c.country
ORDER BY 
    total_revenue DESC;

--graf 3
SELECT 
    t.artist AS artist,
    SUM(fs.unit_price * fs.quantity) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_track t ON fs.track_id = t.dim_trackid
GROUP BY 
    t.artist
ORDER BY 
    total_revenue DESC
LIMIT 10;

--graf 4
SELECT 
    t.album AS album,
    SUM(fs.unit_price * fs.quantity) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_track t ON fs.track_id = t.dim_trackid
GROUP BY 
    t.album
ORDER BY 
    total_revenue DESC;

--graf 5
SELECT 
    d.date AS sale_date,
    SUM(fs.unit_price * fs.quantity) AS total_revenue
FROM 
    fact_sales fs
JOIN 
    dim_date d ON fs.date_id = d.dim_dateid
GROUP BY 
    d.date
ORDER BY 
    d.date;
