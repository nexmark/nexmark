-- -------------------------------------------------------------------------------------------------
-- Query 0: Pass through (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- This measures the monitoring overhead of the Flink SQL implementation including the source generator.
-- Using `bid` events here, as they are most numerous with default configuration.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q0 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q0
SELECT auction, bidder, price, `dateTime`, extra FROM bid;