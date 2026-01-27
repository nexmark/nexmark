-- -------------------------------------------------------------------------------------------------
-- Query 10: Log to File System (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Log all events to file system. Illustrates windows streaming data into partitioned file system.
--
-- Every minute, save all events from the last period into partitioned log files.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q10 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  dt STRING,
  hm STRING
) PARTITIONED BY (dt, hm) WITH (
  ${SINK_DDL}
);

INSERT INTO nexmark_q10
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;