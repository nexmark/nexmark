-- -------------------------------------------------------------------------------------------------
-- Query 16: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  category BIGINT,
  `day` VARCHAR,
  `hour` VARCHAR,
  total_bids BIGINT,
  rank1_bids BIGINT,
  rank2_bids BIGINT,
  rank3_bids BIGINT,
  total_bidders BIGINT,
  rank1_bidders BIGINT,
  rank2_bidders BIGINT,
  rank3_bidders BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
     category,
     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
     DATE_FORMAT(dateTime, 'HH') as `hour`,
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     count(distinct bidder) AS total_bidders,
     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders
FROM bid
GROUP BY category, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), DATE_FORMAT(dateTime, 'HH');