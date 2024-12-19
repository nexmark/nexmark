-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q9 (
  id  BIGINT,
  itemName  VARCHAR,
  description  VARCHAR,
  initialBid  BIGINT,
  reserve  BIGINT,
  customTime  TIMESTAMP(3),
  expires  TIMESTAMP(3),
  seller  BIGINT,
  category  BIGINT,
  extra  VARCHAR,
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  bid_customTime  TIMESTAMP(3),
  bid_extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q9
SELECT
    id, itemName, description, initialBid, reserve, customTime, expires, seller, category, extra,
    auction, bidder, price, bid_customTime, bid_extra
FROM (
   SELECT A.*, B.auction, B.bidder, B.price, B.customTime AS bid_customTime, B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.customTime ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B.customTime BETWEEN A.customTime AND A.expires
)
WHERE rownum <= 1;