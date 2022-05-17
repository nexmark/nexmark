-- -------------------------------------------------------------------------------------------------
-- Query 4: Price drop within an interval (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Detects a price drop of 50 that happens within an interval of 5 second time window.
-- Illustrates WITHIN clause in MATCH_RECOGNIZE.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
      auction  BIGINT,
      bidder  BIGINT,
      drop_time  TIMESTAMP(3),
      drop_diff  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction, bidder, drop_time, drop_diff
FROM bid
MATCH_RECOGNIZE(
    PARTITION BY auction, bidder
    ORDER BY dateTime
    MEASURES
        C.dateTime AS drop_time,
        A.price - C.price AS drop_diff
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B* C) WITHIN INTERVAL '5' SECOND
    DEFINE
        B AS B.price > A.price - 50,
        C AS C.price < A.price - 50
);
