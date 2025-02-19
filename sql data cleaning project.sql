-- DATA CLEANING

SELECT*
FROM layoffs;

-- 1. remove any duplicates
-- 2. standardize the data
-- 3. null/blank values
-- 4. remove columns


CREATE TABLE layoff_table
LIKE layoffs;

SELECT*
FROM layoff_table;


INSERT layoff_table
SELECT*
FROM layoffs;


SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,  percentage_laid_off, `date`,  stage, country, funds_raised_millions  ) AS row_num
FROM layoff_table;

WITH duplicate_cte AS 
(SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,  percentage_laid_off, `date`, stage, country, funds_raised_millions  ) AS row_num
FROM layoff_table
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT*
FROM layoff_table
WHERE company = 'Absci';

SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,  percentage_laid_off, `date`,  stage, country, funds_raised_millions  ) AS row_num
FROM layoff_table;

WITH duplicate_cte AS 
(SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,  percentage_laid_off, `date`, stage, country, funds_raised_millions  ) AS row_num
FROM layoff_table
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;



CREATE TABLE `layoff_table2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT*
FROM layoff_table2
WHERE row_num > 1;

INSERT INTO layoff_table2
(SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,  percentage_laid_off, `date`, stage, country, funds_raised_millions  ) AS row_num
FROM layoff_table
);

DELETE
FROM layoff_table2
WHERE row_num > 1;

SELECT*
FROM layoff_table2;

-- Standardizing data

SELECT company, TRIM(company)
FROM layoff_table2;

UPDATE layoff_table2
SET company = TRIM(company);

SELECT *
FROM layoff_table2
WHERE industry LIKE 'crypto%';

UPDATE layoff_table2
SET industry = 'crtpto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT country
FROM layoff_table2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING'.' FROM COUNTRY)
FROM layoff_table2
ORDER BY 1;

UPDATE layoff_table2
SET country =  TRIM(TRAILING'.' FROM COUNTRY)
WHERE country LIKE 'united states%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoff_table2;

UPDATE layoff_table2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoff_table2
MODIFY COLUMN `date` DATE;

SELECT*
FROM layoff_table2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoff_table2
WHERE company = 'Airbnb';

UPDATE layoff_table2
SET industry = null
WHERE industry = ' ';



SELECT *
FROM layoff_table2  AS t1
JOIN layoff_table2 AS  t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = ' ' )
AND t2.industry IS NOT NULL;


SELECT *
FROM layoff_table2
WHERE industry = '' OR industry IS NULL;

UPDATE layoff_table2
SET industry = NULL
WHERE industry = '';

UPDATE layoff_table2 t1
JOIN layoff_table2 AS  t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry  IS NOT NULL;

SELECT*
FROM layoff_table2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoff_table2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT*
FROM layoff_table2;

ALTER TABLE layoff_table2
DROP COLUMN row_num










