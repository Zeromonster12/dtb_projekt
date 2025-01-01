CREATE DATABASE SCORPION_CHINOOK_DB;
CREATE SCHEMA STAGING;
CREATE STAGE my_stage;

USE DATABASE SCORPION_CHINOOK_DB;
USE SCHEMA SCORPION_CHINOOK_DB.STAGING;


CREATE TABLE IF NOT EXISTS track_staging (
  trackid INT PRIMARY KEY,
  name VARCHAR(200),
  albumid INT,
  mediatypeid INT,
  genreid INT,
  composer VARCHAR(220),
  miliseconds INT,
  bytes INT,
  unitprice DECIMAL(10,2),
  FOREIGN KEY (albumid) REFERENCES album_staging(albumid),
  FOREIGN KEY (mediatypeid) REFERENCES mediatype_staging(mediatypeid),
  FOREIGN KEY (genreid) REFERENCES genre_staging(genreid)
);

CREATE TABLE IF NOT EXISTS artist_staging (
  artistid INT PRIMARY KEY,
  name VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS album_staging (
  albumid INT PRIMARY KEY,
  title VARCHAR(160),
  artistid INT,
  FOREIGN KEY (artistid) REFERENCES artist_staging(artistid)
);

CREATE TABLE IF NOT EXISTS mediatype_staging (
  mediatypeid INT PRIMARY KEY,
  name VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS genre_staging (
  genreid INT PRIMARY KEY,
  name VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS employee_staging (
  employeeid INT PRIMARY KEY,
  lastname VARCHAR(20),
  firstname VARCHAR(20),
  title VARCHAR(30),
  reportsto INT,
  birthdate DATETIME,
  hiredate DATETIME,
  address VARCHAR(70),
  city VARCHAR(40),
  state VARCHAR(40),
  country VARCHAR(40),
  postalcode VARCHAR(10),
  phone VARCHAR(24),
  fax VARCHAR(24),
  email VARCHAR(60),
  FOREIGN KEY (reportsto) REFERENCES employee_staging(employeeid)
);

CREATE TABLE IF NOT EXISTS customer_staging (
  customerid INT PRIMARY KEY,
  firstname VARCHAR(40),
  lastname VARCHAR(20),
  company VARCHAR(80),
  address VARCHAR(70),
  city VARCHAR(40),
  state VARCHAR(40),
  country VARCHAR(40),
  postalcode VARCHAR(10),
  phone VARCHAR(24),
  fax VARCHAR(24),
  email VARCHAR(60),
  supportrepid INT,
  FOREIGN KEY (supportrepid) REFERENCES employee_staging(employeeid)
);

CREATE TABLE IF NOT EXISTS invoice_staging (
  invoiceid INT PRIMARY KEY,
  customerid INT,
  invoicedate DATETIME,
  billingaddress VARCHAR(70),
  billingcity VARCHAR(40),
  billingstate VARCHAR(40),
  billingcountry VARCHAR(40),
  billingpostalcode VARCHAR(10),
  total DECIMAL(10,2),
  FOREIGN KEY (customerid) REFERENCES customer_staging(customerid)
);

CREATE TABLE IF NOT EXISTS invoiceline_staging (
  invoicelineid INT PRIMARY KEY,
  invoiceid INT,
  trackid INT,
  unitprice DECIMAL(10,2),
  quantity INT,
  FOREIGN KEY (invoiceid) REFERENCES invoice_staging(invoiceid),
  FOREIGN KEY (trackid) REFERENCES track_staging(trackid)
);

CREATE TABLE IF NOT EXISTS playlist_staging (
  playlistid INT PRIMARY KEY,
  name VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS playlisttrack_staging (
  playlistid INT,
  trackid INT,
  PRIMARY KEY (playlistid, trackid),
  FOREIGN KEY (playlistid) REFERENCES playlist_staging(playlistid),
  FOREIGN KEY (trackid) REFERENCES track_staging(trackid)
);

LIST @my_stage;

COPY INTO track_staging
FROM @my_stage/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO artist_staging
FROM @my_stage/artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO album_staging
FROM @my_stage/album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO mediatype_staging
FROM @my_stage/mediatype.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genre_staging
FROM @my_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employee_staging
FROM @my_stage/employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO customer_staging
FROM @my_stage/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoice_staging
FROM @my_stage/invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoiceline_staging
FROM @my_stage/invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlist_staging
FROM @my_stage/playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlisttrack_staging
FROM @my_stage/playlisttrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

------------------------------------------

CREATE TABLE dim_track AS
SELECT DISTINCT
    t.trackid AS dim_trackid,
    t.name AS track_name,
    t.composer AS composer,
    t.miliseconds AS duration_ms,
    t.bytes AS size_bytes,
    t.unitprice AS price,
    g.name AS genre,
    a.title AS album,
    m.name AS media_type,
    ar.name AS artist
FROM track_staging t
JOIN genre_staging g ON t.genreid = g.genreid
JOIN album_staging a ON t.albumid = a.albumid
JOIN mediatype_staging m ON t.mediatypeid = m.mediatypeid
JOIN artist_staging ar ON a.artistid = ar.artistid;

CREATE TABLE dim_employee AS
SELECT DISTINCT
    e.employeeid AS dim_employeeid,
    e.lastname AS last_name,
    e.firstname AS first_name,
    e.title AS title,
    e.city AS city,
    e.country AS country
FROM employee_staging e;

CREATE TABLE dim_customer AS
SELECT DISTINCT
    c.customerid AS dim_customerid,
    c.firstname AS first_name,
    c.lastname AS last_name,
    c.city AS city,
    c.country AS country,
    c.email AS email
FROM customer_staging c;
CREATE TABLE dim_invoice AS
SELECT DISTINCT
    i.invoiceid AS dim_invoiceid,
    i.invoicedate AS invoice_date,
    i.total AS total_amount,
    c.country AS country
FROM invoice_staging i
JOIN customer_staging c ON i.customerid = c.customerid;

CREATE TABLE dim_date AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY CAST(i.invoicedate AS DATE)) AS dim_dateid,
    CAST(i.invoicedate AS DATE) AS date,
    DATE_PART('day', i.invoicedate) AS day,
    DATE_PART('month', i.invoicedate) AS month,
    DATE_PART('year', i.invoicedate) AS year,
    DATE_PART('quarter', i.invoicedate) AS quarter
FROM invoice_staging i;
drop table fact_sales;

CREATE TABLE fact_sales AS
SELECT
    il.invoicelineid AS fact_salesid,   
    i.invoiceid AS invoice_id,         
    il.quantity AS quantity,          
    il.unitprice AS unit_price,      
    t.trackid AS track_id,            
    c.customerid AS customer_id,       
    e.employeeid AS employee_id,        
    d.dim_dateid AS date_id         
FROM invoiceline_staging il
JOIN invoice_staging i ON il.invoiceid = i.invoiceid 
JOIN track_staging t ON il.trackid = t.trackid  
JOIN customer_staging c ON i.customerid = c.customerid 
JOIN employee_staging e ON c.supportrepid = e.employeeid
JOIN dim_date d ON CAST(i.invoicedate AS DATE) = d.date; 

select * from fact_sales;














