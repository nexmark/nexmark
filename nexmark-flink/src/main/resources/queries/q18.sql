-- -------------------------------------------------------------------------------------------------
-- Query 16: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  category BIGINT,
  `day` VARCHAR,
  bidder BIGINT,
  min_visit_time1 BIGINT,
  min_visit_time2 BIGINT,
  min_visit_time3 BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
     category,
     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
     bidder,
     MIN(UNIX_TIMESTAMP(CAST(dateTime AS VARCHAR))) filter (where price < 10000) AS min_visit_time1,
     MIN(UNIX_TIMESTAMP(CAST(dateTime AS VARCHAR))) filter (where price >= 10000 and price < 1000000) AS min_visit_time2,
     MIN(UNIX_TIMESTAMP(CAST(dateTime AS VARCHAR))) filter (where price >= 1000000) AS min_visit_time3
FROM bid
GROUP BY category, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), bidder;