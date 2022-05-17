-- -------------------------------------------------------------------------------------------------
-- Query 0: Periods of a constantly decreasing price (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Finds periods of a constantly decreasing price of a single bidder.
-- Illustrates a typical Pattern in MATCH_RECOGNIZE.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
      auction  BIGINT,
      bidder  BIGINT,
      start_tstamp  TIMESTAMP(3),
      bottom_tstamp  TIMESTAMP(3),
      end_tstamp  TIMESTAMP(3)
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction, bidder, start_tstamp, bottom_tstamp, end_tstamp
FROM bid
MATCH_RECOGNIZE (
    PARTITION BY auction, bidder
    ORDER BY dateTime
    MEASURES
        START_ROW.dateTime AS start_tstamp,
        LAST(PRICE_DOWN.dateTime) AS bottom_tstamp,
        LAST(PRICE_UP.dateTime) AS end_tstamp
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST PRICE_UP
    PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)
    DEFINE
        PRICE_DOWN AS
            (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR
                PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),
        PRICE_UP AS
            PRICE_UP.price > LAST(PRICE_DOWN.price, 1)
);
