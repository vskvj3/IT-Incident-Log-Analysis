-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("CapstoneVisualizations") \
-- MAGIC     .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
-- MAGIC     .config("fs.azure.account.key.ustcapstonestorage.blob.core.windows.net", "<pass>") \
-- MAGIC     .getOrCreate()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC data_df = spark.read.parquet("wasbs://capstonetransformed@ustcapstonestorage.blob.core.windows.net/transformed.parquet")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Materialized View
-- MAGIC data_df.createOrReplaceTempView("auditdata")

-- COMMAND ----------

select * 
from auditdata

-- COMMAND ----------

select count(*) as total_records
from auditdata

-- COMMAND ----------

select count(distinct number) as total_incidents
from auditdata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC currently active incidents vs currently closed incidents

-- COMMAND ----------

select number, active
from auditdata

-- COMMAND ----------

SELECT 
  min(DATE_FORMAT(opened_at, 'yyyy-MM')),
  max(DATE_FORMAT(closed_at, 'yyyy-MM'))
FROM auditdata

-- COMMAND ----------

select distinct incident_state
from auditdata
where incident_state != '-100'

-- COMMAND ----------

select distinct priority
from auditdata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### top 5 closed code

-- COMMAND ----------

select closed_code, count(distinct number) as count
from auditdata
where incident_state = 'Closed'
group by closed_code
order by count desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Span of Incident Reporting

-- COMMAND ----------

SELECT DATE_FORMAT(opened_at, 'yyyy-MM') AS month, COUNT(*) AS opened_incidents
FROM auditdata
GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
ORDER BY month;

-- COMMAND ----------

SELECT DATE_FORMAT(opened_at, 'yyyy-MM') AS month, COUNT(*) AS opened_incidents
FROM auditdata
WHERE opened_year = 2016 and (opened_month >= 2 and opened_month <= 6)
GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
ORDER BY month;

-- COMMAND ----------

SELECT DATE_FORMAT(closed_at, 'yyyy-MM') AS month, COUNT(*) AS closed_incidents
FROM auditdata
WHERE closed_year = 2016 and (closed_month >= 2 and closed_month <= 6)
GROUP BY DATE_FORMAT(closed_at, 'yyyy-MM')
ORDER BY month;

-- COMMAND ----------

-- Weekly Incidents
SELECT date_format(opened_at, 'EEEE') AS opened_week, COUNT(*) AS opened_incidents
FROM auditdata
WHERE incident_state = 'Active'
GROUP BY opened_week
ORDER BY 
  CASE date_format(opened_at, 'EEEE')
    WHEN 'Monday' THEN 1
    WHEN 'Tuesday' THEN 2
    WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4
    WHEN 'Friday' THEN 5
    WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Impact VS Days to Resolve

-- COMMAND ----------

select impact, AVG(CAST(total_seconds / (60 * 60 * 24) AS INT)) AS avg_days
from auditdata
where incident_state != 'Closed'
group by impact

-- COMMAND ----------

SELECT 
  DATE_FORMAT(opened_at, 'yyyy-MM') AS month,
  COUNT(*) AS opened_incidents,
  SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) AS resolved_incidents,
  SUM(CASE WHEN closed_at IS NOT NULL THEN 1 ELSE 0 END) AS closed_incidents
FROM auditdata
GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
ORDER BY month;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Overalll SLA Adherence Rate

-- COMMAND ----------

SELECT 
  SUM(CASE WHEN made_sla = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS sla_adherence_rate,
  COUNT(distinct number) AS total_incidents
FROM auditdata


-- COMMAND ----------

SELECT 
  SUM(CASE WHEN made_sla = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS SLA_Made,
  SUM(CASE WHEN made_sla = TRUE THEN 0 ELSE 1 END) / COUNT(*) * 100 AS SLA_Not_Made
FROM auditdata


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SLA Adherence in Closed Events

-- COMMAND ----------

SELECT 
  CASE WHEN made_sla = true THEN 'Made_SLA' 
       ELSE 'Not_Made_SLA'
  END AS status, 
  COUNT(number) AS count
FROM auditdata
where incident_state = 'Closed'
GROUP BY made_sla

-- COMMAND ----------

select number, count(*) as count
from (
select number, made_sla
from auditdata
group by number, made_sla
) as _
group by number

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SLA Compliance by Priority for each event:

-- COMMAND ----------

SELECT 
  priority, 
  COUNT(*) AS total_incidents,
  SUM(CASE WHEN made_sla = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS sla_adherence_rate
FROM auditdata
GROUP BY priority
ORDER BY sla_adherence_rate DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SLA Compliance by Urgency for each event:

-- COMMAND ----------

SELECT 
  urgency, 
  COUNT(*) AS total_incidents,
  SUM(CASE WHEN made_sla = TRUE THEN 1 ELSE 0 END) / COUNT(distinct number) * 100 AS sla_adherence_rate
FROM auditdata
GROUP BY urgency
ORDER BY sla_adherence_rate DESC;

-- COMMAND ----------

SELECT 
  impact, 
  COUNT(*) AS total_incidents,
  SUM(CASE WHEN made_sla = TRUE THEN 1 ELSE 0 END) / COUNT(*) * 100 AS sla_adherence_rate
FROM auditdata
GROUP BY impact
ORDER BY sla_adherence_rate DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC --------------------------

-- COMMAND ----------

select number, incident_state, count(distinct number) as closed, max(reopen_count)
from auditdata
group by number, incident_state
having incident_state = 'Closed'
order by closed desc

-- COMMAND ----------

select priority, count(*) as count
from auditdata
group by priority

-- COMMAND ----------

select incident_state, count(*) as count
from auditdata
where incident_state != '-100'
group by incident_state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Symptom based analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top 10 Most Common Symptoms

-- COMMAND ----------

select u_symptom, count(*) as count
from auditdata
where u_symptom != 'Unknown'
group by u_symptom
order by count desc
limit 10

-- COMMAND ----------

-- 10 Most common symptoms among New incidents

select u_symptom, count(*) as count
from auditdata
where u_symptom != 'Unknown' AND incident_state = 'New'
group by u_symptom
order by count desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average Resolution Time

-- COMMAND ----------

SELECT 
  AVG(DATEDIFF(HOUR, opened_at, resolved_at)) AS avg_resolution_time_hours
FROM auditdata
WHERE incident_state = 'Resolved' AND
resolved_at IS NOT NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reopen Count for each Incidents

-- COMMAND ----------

SELECT 
  DISTINCT number, 
  CASE WHEN MAX(reopen_count) > 0 THEN 'Reopened' ELSE 'Not Reopened'  END as reopened
FROM auditdata
WHERE resolved_at IS NOT NULL
GROUP BY number


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incident Distribution

-- COMMAND ----------

SELECT 
  impact, 
  urgency, 
  priority, 
  COUNT(*) AS incident_count
FROM auditdata
GROUP BY impact, urgency, priority
ORDER BY incident_count DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incidents By Contact Type

-- COMMAND ----------

select contact_type, count(distinct number) as count
from auditdata
group by contact_type
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average Resolution Time By Impact

-- COMMAND ----------

SELECT 
  impact, 
  AVG(DATEDIFF(DAY, opened_at, resolved_at)) AS avg_resolution_time_hours,
  COUNT(*) AS total_incidents
FROM auditdata
WHERE incident_state = 'Resolved' AND resolved_at IS NOT NULL
GROUP BY impact
ORDER BY avg_resolution_time_hours ASC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average Resolution Time By Urgency

-- COMMAND ----------

SELECT 
  urgency, 
  AVG(DATEDIFF(HOUR, opened_at, resolved_at)) AS avg_resolution_time_hours,
  COUNT(*) AS total_incidents
FROM auditdata
WHERE incident_state = 'Resolved' AND resolved_at IS NOT NULL
GROUP BY urgency
ORDER BY avg_resolution_time_hours ASC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Average Resolution Time By Priority

-- COMMAND ----------

SELECT 
  priority, 
  AVG(DATEDIFF(HOUR, opened_at, resolved_at)) AS avg_resolution_time_hours,
  COUNT(*) AS total_incidents
FROM auditdata
WHERE incident_state = 'Resolved' AND resolved_at IS NOT NULL
GROUP BY priority
ORDER BY avg_resolution_time_hours ASC;


-- COMMAND ----------

select closed_month, count(*) as count
from auditdata
where incident_state = 'Closed'
group by closed_month
order by closed_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Trend analysis of query generated vs query closed
-- MAGIC - correlation between symptoms and priority

-- COMMAND ----------

select u_symptom, priority, count(distinct number) as count
from auditdata
where u_symptom != 'Unknown'
group by u_symptom, priority
order by count desc

-- COMMAND ----------

select opened_month, closed_month, count(*)
from auditdata
group by opened_month, closed_month

-- COMMAND ----------

select  closed_month, opened_month, count(distinct number) as count
from auditdata
group by closed_month, opened_month

-- COMMAND ----------

select distinct number, caller_id, opened_by
from auditdata
where contact_type = 'Self service'

-- COMMAND ----------

select contact_type, count(distinct number)
from auditdata
group by contact_type

-- COMMAND ----------



-- COMMAND ----------

select priority, incident_state, count(*) as count
from auditdata
where incident_state != '-100'
group by priority, incident_state


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incidents Trend Analysis

-- COMMAND ----------


SELECT 
    DATE_FORMAT(opened_at, 'yyyy-MM') AS month,
    COUNT(*) AS opened_count
FROM 
    auditdata
WHERE 
    opened_at >= '2016-02-01' AND opened_at < '2016-07-01'
GROUP BY 
        DATE_FORMAT(opened_at, 'yyyy-MM')


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top 10 Resolvers

-- COMMAND ----------

select resolved_by, count(distinct number) as count
from auditdata
group by resolved_by
order by count desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top 10 Most Affected Locations

-- COMMAND ----------

select location, count(distinct number) as count
from auditdata
group by location
order by count desc
limit 10
