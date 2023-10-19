-- How many animals of each type have outcomes?
SELECT "Animal Type", COUNT(DISTINCT "Animal ID") as "Count"
FROM outcomes
GROUP BY "Animal Type";

-- How many animals are there with more than 1 outcome?
SELECT COUNT(*) as "Count"
FROM (
    SELECT "Animal ID"
    FROM outcomes
    GROUP BY "Animal ID"
    HAVING COUNT(*) > 1
) AS multi_outcome_animals;

-- What are the top 5 months for outcomes?
SELECT EXTRACT(MONTH FROM "DateTime"::timestamp) as "Month", COUNT(*) as "Count"
FROM outcomes
GROUP BY "Month"
ORDER BY "Count" DESC
LIMIT 5;


-- What is the total number percentage of kittens, adults, and seniors, whose outcome is "Adopted"?
SELECT "Age Group", 
       COUNT(CASE WHEN "Outcome Type" = 'Adopted' THEN 1 ELSE NULL END) as "Adopted",
       COUNT(*) as "Total",
       ROUND(COUNT(CASE WHEN "Outcome Type" = 'Adopted' THEN 1 ELSE NULL END) / COUNT(*) * 100, 2) as "Percentage"
FROM (
    SELECT "Animal ID",
           CASE 
               WHEN "Animal Type" = 'Cat' AND "Age upon Outcome" < '1 year' THEN 'Kitten'
               WHEN "Animal Type" = 'Cat' AND "Age upon Outcome" BETWEEN '1 year' AND '10 years' THEN 'Adult'
               WHEN "Animal Type" = 'Cat' AND "Age upon Outcome" > '10 years' THEN 'Senior'
           END as "Age Group",
           "Outcome Type"
    FROM outcomes
) as age_groups
GROUP BY "Age Group"
ORDER BY "Age Group";

-- Among all the cats who were "Adopted", what is the total number percentage of kittens, adults, and seniors?
SELECT "Age Group", 
       COUNT(CASE WHEN "Outcome Type" = 'Adopted' THEN 1 ELSE NULL END) as "Adopted",
       COUNT(*) as "Total",
       ROUND(COUNT(CASE WHEN "Outcome Type" = 'Adopted' THEN 1 ELSE NULL END) / COUNT(*) * 100, 2) as "Percentage"
FROM (
    SELECT "Animal ID",
           CASE 
               WHEN "Animal Type" = 'Cat' AND "Age upon Outcome" < '1 year' THEN 'Kitten'
               WHEN "Animal Type" = 'Cat' AND "Age upon Outcome" BETWEEN '1 year' AND '10 years' THEN 'Adult'
               WHEN "Animal Type" = 'Cat' AND "Age upon Outcome" > '10 years' THEN 'Senior'
           END as "Age Group",
           "Outcome Type"
    FROM outcomes
    WHERE "Outcome Type" = 'Adoption'
) as adopted_cats
GROUP BY "Age Group"
ORDER BY "Age Group";

-- For each date, what is the cumulative total of outcomes up to and including this date?
SELECT DISTINCT "Date",
       COUNT(*) OVER (ORDER BY "Date") as "Cumulative Total"
FROM (
    SELECT DISTINCT DATE("DateTime") as "Date"
    FROM outcomes
) as unique_dates;
