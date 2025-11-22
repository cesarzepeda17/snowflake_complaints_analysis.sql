# snowflake_complaints_analysis.sql
Explain the project goal (e.g., "Analyze public consumer complaints data").

/* Create a new database called "COMPLAINTS". */
/* Create a new worksheet for the final project. */
/* Create a new table for complaint details. */
CREATE TABLE complaint_details
(
complaint_id INTEGER,
date_received DATE,
submitted_via STRING,
product STRING,
subproduct STRING,
issue STRING,
subissue STRING,
consumer_complaint_narrative STRING,
consumer_consent_provided STRING,
company_name STRING,
company_public_response STRING,
company_response_to_consumer STRING,
timely_response STRING,
date_sent_to_company DATE,
state STRING,
zip_code INTEGER
);
/* Create a staging area that points to the S3 bucket. */
CREATE OR REPLACE STAGE complaints
url = 's3://cfpb-complaints';
/* List the files in the staging area (S3). */
LIST @complaints;
/* Define the format of CSV files to be loaded. */
CREATE OR REPLACE FILE FORMAT csv_format
type = csv
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
null_if = ('')
field_optionally_enclosed_by = '\042';
/* Load the complaint details. */
COPY INTO complaint_details
FROM @complaints/cfpb_complaints
file_format = csv_format;
/* Determine if complaint IDs are unique in the table. */
SELECT COUNT(complaint_id) FROM complaint_details;
SELECT COUNT(DISTINCT complaint_id) FROM complaint_details;


-- Explore the Table
SELECT
    *
FROM
    complaint_details
LIMIT
    10;

SELECT
    COUNT(*)
FROM
    complaint_details;

/* QUERY 1 : Based on this complaint data, when was the first complaint received? Refer to the "date_received" field rather than the "date_sent_to_company" field. 
Provide the date in the field below and use the following format: YY-MM-DD. As an example, 2102-05-02, if that was the correct answer */
SELECT
    date_received,
FROM
    complaint_details
GROUP BY
    date_received
ORDER BY
    date_received ASC;

/* QUERY 2: What financial products did we recieve the most complains about? When making this determination, use the "subproduct"  instead of the "product" for more detail */
SELECT
    subproduct,
    COUNT(subproduct) AS total_subproducts
FROM
    complaint_details
GROUP BY
    subproduct
ORDER BY
    total_subproducts DESC;

/* Query 3 : What company received the most complaints about cryptocurrencies (or, in other words, the "virtual currency" subproduct) in 2019? When writing the statement, I recommend using the YEAR function. Also, you will need to use the date that the complaint was acutally sent to the comany, NOT when it was recevied.*/

WITH CryptoComplaint2019 AS(
    SELECT
        company_name
    FROM
        complaint_details
    WHERE
        subproduct = 'Virtual currency'
    AND 
        YEAR(date_received) = 2019
)
SELECT
    company_name,
    COUNT(company_name) AS total_complaints
FROM
    CryptoComplaint2019
GROUP BY
    company_name
ORDER BY
    total_complaints DESC;

/* Query 4: Compare the number of complaints recieved each month with the number of complaints recieved in the previous month. (They should be side-by-side in the results.) When performing this calculation, use "date_recieved" as the date.*/

-- The total complaints each month of year 2018
WITH TotalComplaints AS(
SELECT
    COUNT(*) AS total_complaints,
    MONTH(date_received) AS month,
    YEAR(date_received) AS year
FROM
    complaint_details
GROUP BY
    month,
    year
HAVING
    year = 2018

)

SELECT
    month,
    year,
    total_complaints,
    LAG(total_complaints) OVER(ORDER BY month ASC) AS PreviousComplaint, -- Previous month of complaints
    (total_complaints - PreviousComplaint) AS Difference_of_complaints
FROM
    TotalComplaints
ORDER BY
    month ASC;

/* Query 5: What companies are doing a poor job of responding to compaints in a timely manner? To answer this question, prepare a query that counts the number of total complaints, the number of untimely responses, and the percentage of untimely responses by company. (For the last metric, take the untimely responses and divided by the total number of complaints.) Also, in the results, only show companies that had at least 200 complaintsin the dataset. HINT: to count the number of untimely responses, toy can use the following in your SELECT statementL COUNT(CASE WHEN timely)_response = 'FALSE' THEN 1 END ) AS untimely_complaints*/

WITH UntimelyComplaints AS ( 
    SELECT 
        company_name,
        -- Counts the number of untimely responses per company
        COUNT(CASE WHEN timely_response = 'FALSE' THEN 1 END) AS untimely_complaints
    FROM 
        complaint_details 
    GROUP BY 
        company_name 
) 
, TotalComplaints AS (
    SELECT 
        company_name,
        -- Counts the total number of complaints per company
        COUNT(*) AS total_complaints 
    FROM 
        complaint_details 
    GROUP BY 
        company_name 
) 
SELECT 
    U.company_name,
    T.total_complaints,
    U.untimely_complaints,
    -- Calculate the percentage of untimely responses
    (CAST(U.untimely_complaints AS NUMERIC) / T.total_complaints) AS untimely_percent
FROM 
    UntimelyComplaints U 
INNER JOIN 
    TotalComplaints T ON U.company_name = T.company_name
WHERE 
    T.total_complaints >= 200
ORDER BY 
    untimely_percent DESC;



--PRACTICE QUESTIONS
/*Question 1: Timely Response Count Write a query to count the total number of complaints where the timely_response field is 
'TRUE' versus where it is 'FALSE' from the complaint_details table. Order the results by the count in descending order.*/


SELECT
    timely_response,
    COUNT(*) AS total_complaints
FROM
    complaint_details
GROUP BY
    timely_response
ORDER BY
    total_complaints DESC;

/*Question 2: Complaint Response Status Assume you have a second table, Complaint_Type, with columns product and type_description. Write a query that performs an INNER JOIN between complaint_details and Complaint_Type on the product column. Use a CASE WHEN statement to categorize each complaint based on its product:

If product is 'Mortgage', categorize it as 'Housing Finance'.

If product is 'Credit card', categorize it as 'Consumer Lending'.

Otherwise, categorize it as 'Other Consumer Product'.

Return the new category and the total number of complaints in that category.*/

SELECT
    CASE
        WHEN product = 'Mortgage' THEN 'Housing Finance'
        WHEN product = 'Credit card or prepaid card' THEN 'Consumer Lending'
        ELSE 'Other Consumer Product'
    END AS ComplaintType,
    COUNT(*) AS Total_Complaints
FROM
    complaint_details
GROUP BY
    ComplaintType
ORDER BY
    Total_Complaints DESC;

/* Question 3: Top Complained-About Products Using a Common Table Expression (CTE) or a Subquery, find the top 5 product categories in the complaint_details table with the highest total number of complaints. Then, in the main query, filter the results to only show records where the product is one of those top 5. */

WITH ComplaintProductType AS(
    SELECT
        COUNT(*) AS Total_Complaint,
        product
    FROM
        complaint_details
    GROUP BY
        product
    ORDER BY
        Total_Complaint DESC
    LIMIT
        5
)
SELECT
    product,
    Total_Complaint
FROM
    complaintproducttype
LIMIT
    1;

/* Question 4: Month-over-Month Complaint Comparison (LEAD/LAG) Write a query to show the total number of complaints received each month from the complaint_details table. In addition to the current month's complaint count, include a new column that shows the number of complaints received in the following month. This requires using a Window Function like LEAD() . */

WITH MonthlyComplaints AS(
    SELECT
        COUNT(*) AS TotalComplaints,
        MONTH(date_received) AS month
    FROM
        complaint_details
    GROUP BY
        month
)
SELECT
    month,
    TotalComplaints,
    LEAD(TotalComplaints) OVER(ORDER BY month ASC) AS NextMonthComplaints
FROM
    MONTHLYCOMPLAINTS
ORDER BY
    month;
