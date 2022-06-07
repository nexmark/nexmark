-- -------------------------------------------------------------------------------------------------
-- Query 1: Longest period of time for average price (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Finds the longest period of time for which the average price of a bidder did not go below certain threshold.
-- Illustrates Aggregations and  a AFTER MATCH strategy SKIP PAST LAST ROW in MATCH_RECOGNIZE.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
      auction  BIGINT,
      bidder  BIGINT,
      start_tstamp  TIMESTAMP(3),
      end_tstamp  TIMESTAMP(3),
      avg_price  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction, bidder, start_tstamp, end_tstamp, avg_price
FROM bid
MATCH_RECOGNIZE(
    PARTITION BY auction, bidder
    ORDER BY dateTime
    MEASURES
        FIRST(A.dateTime) AS start_tstamp,
        LAST(A.dateTime) AS end_tstamp,
        AVG(A.price) AS avg_price
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A+ B)
    DEFINE
        A AS AVG(A.price) < 10000
);
