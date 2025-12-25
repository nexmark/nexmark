-- -------------------------------------------------------------------------------------------------
-- Query 23: Expand bid with person and auction (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find all bids made by a person who has also listed an item for auction
-- Illustrates a multi-way join.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q23
(
    bidder           BIGINT,
    price            BIGINT,
    channel          VARCHAR,
    url              VARCHAR,
    bid_extra        VARCHAR,

    person_id       BIGINT,
    name             VARCHAR,
    emailAddress     VARCHAR,
    creditCard       VARCHAR,
    city             VARCHAR,
    state            VARCHAR,
    person_extra     VARCHAR,

    itemName         VARCHAR,
    description      VARCHAR,
    initialBid       BIGINT,
    reserve          BIGINT,
    auction_dateTime TIMESTAMP(3),
    expires          TIMESTAMP(3),
    seller           BIGINT,
    category         BIGINT,
    auction_extra    VARCHAR
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO nexmark_q23
SELECT bidder,
       price,
       channel,
       url,
       B.extra    AS bid_extra,

       P.id       AS person_id,
       name,
       emailAddress,
       creditCard,
       city,
       state,
       P.extra    AS person_extra,
       itemName,

       description,
       initialBid,
       reserve,
       A.dateTime AS auction_dateTime,
       expires,
       seller,
       category,
       A.extra    AS auction_extra
FROM bid B
         JOIN
     person P ON P.id = B.bidder
         JOIN
     auction A ON A.seller = B.bidder;

