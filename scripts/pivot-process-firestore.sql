/* This will live in a stored procedure, which gets triggered by the query_js cloud function (same one that previously contained the full query)
*/
CREATE OR REPLACE PROCEDURE `josh-crowhurt-personal-bq.firestore_export.genning_abt_macros_procedure`()
BEGIN
CALL firestore_export.pivot(
  'firestore_export.posts_schema_schema_latest',
  'firestore_export.changelog_pivot',
  ['timestamp','date','flag'],
  'type', 'value', 30, 'SUM', ''
);

CREATE OR REPLACE TABLE firestore_export.ABT (
  Date_Local DATE,
  Active_Calories NUMERIC,
  Resting_Calories NUMERIC,
  Total_Calories_Burned NUMERIC,
  Carbs NUMERIC,
  Calories_Ingested_Carbs NUMERIC,
  Protein NUMERIC,
  Calories_Ingested_Protein NUMERIC,
  Fat NUMERIC,
  Calories_Ingested_Fat NUMERIC,
  Steps NUMERIC,
  Weight NUMERIC,
  Calories_Ingested NUMERIC,
  Lifted NUMERIC,
  Cardio NUMERIC,
  Stretch NUMERIC,
  Calflag NUMERIC,
  Body_Fat_Percentage NUMERIC
);

INSERT INTO firestore_export.ABT SELECT
  DATE(TIMESTAMP_ADD(date, INTERVAL 8 HOUR)) AS Date_Local,
  MAX(e_Active_Calories) Active_Calories,
  MAX(e_Resting_Calories) Resting_Calories,
  MAX(e_Active_Calories) + MAX(e_Resting_Calories) Total_Calories_Burned,
  ROUND(MAX(e_Carbohydrates)) Carbs,
  ROUND(MAX(e_Carbohydrates) * 4) Calories_Ingested_Carbs,
  ROUND(MAX(e_Protein)) Protein,
  ROUND(MAX(e_Protein) * 4) Calories_Ingested_Protein,
  ROUND(MAX(e_Total_Fat)) Fat,
  ROUND(MAX(e_Total_Fat)) * 9 Calories_Ingested_Fat,
  MAX(e_Steps) Steps,
  ROUND(MAX(e_Weight),1) Weight,
  ROUND(MAX(e_Carbohydrates) * 4 + MAX(e_Protein) * 4 + MAX(e_Total_Fat) * 9) Calories_Ingested,
  MAX(e_Lifted) Lifted,
  MAX(e_Cardio) Cardio,
  MAX(e_Stretch) Stretch,
  MAX(e_Calflag) Calflag,
  MAX(e_Body_Fat_Percentage) Body_Fat_Percentage
  FROM `josh-crowhurt-personal-bq.firestore_export.changelog_pivot`
  GROUP BY Date_Local
  ORDER BY Date_Local asc;

CREATE OR REPLACE TABLE firestore_export.Macros (
  Date_Local DATE,
  Macro STRING,
  Grams NUMERIC,
  Calories_Ingested NUMERIC
);

INSERT INTO firestore_export.Macros
  SELECT DATE(TIMESTAMP_ADD(date, INTERVAL 8 HOUR)) AS Date_Local,
  type Macro,
  MAX(value) Grams,
  CASE type
    WHEN 'Total Fat' THEN 9 * MAX(value)
    WHEN 'Protein' THEN 4 * MAX(value)
    WHEN 'Carbohydrates' THEN 4 * MAX(value)
    END Calories_Ingested
  FROM `josh-crowhurt-personal-bq.firestore_export.posts_schema_schema_latest`
  WHERE type IN ('Protein', 'Total Fat', 'Carbohydrates')
  GROUP BY Date_Local, Macro
  ORDER BY Date_Local asc;
END;
