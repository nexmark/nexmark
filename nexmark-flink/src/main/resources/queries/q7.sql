-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  customTime  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q7
SELECT B.auction, B.price, B.bidder, B.customTime, B.extra
from bid B
JOIN (
  SELECT MAX(price) AS maxprice, window_end as customTime
  FROM TABLE(
          TUMBLE(TABLE bid, DESCRIPTOR(customTime), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end
) B1
ON B.price = B1.maxprice
WHERE B.customTime BETWEEN B1.customTime  - INTERVAL '10' SECOND AND B1.customTime;