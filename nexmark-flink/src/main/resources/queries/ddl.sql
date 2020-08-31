CREATE TABLE nexmark (
    event_type int,
    person ROW<
        id  BIGINT,
        name  VARCHAR,
        emailAddress  VARCHAR,
        creditCard  VARCHAR,
        city  VARCHAR,
        state  VARCHAR,
        dateTime TIMESTAMP(3),
        extra  VARCHAR>,
    auction ROW<
        id  BIGINT,
        itemName  VARCHAR,
        description  VARCHAR,
        initialBid  BIGINT,
        reserve  BIGINT,
        dateTime  TIMESTAMP(3),
        expires  TIMESTAMP(3),
        seller  BIGINT,
        category  BIGINT,
        extra  VARCHAR>,
    bid ROW<
        auction  BIGINT,
        bidder  BIGINT,
        price  BIGINT,
        dateTime  TIMESTAMP(3),
        extra  VARCHAR>,
    dateTime AS
        CASE
            WHEN event_type = 0 THEN person.dateTime
            WHEN event_type = 1 THEN auction.dateTime
            ELSE bid.dateTime
        END,
    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND
) WITH (
    'connector' = 'nexmark',
    'first-event.rate' = '${TPS}',
    'next-event.rate' = '${TPS}',
    'person.proportion' = '${PERSON_PROPORTION}',
    'auction.proportion' = '${AUCTION_PROPORTION}',
    'bid.proportion' = '${BID_PROPORTION}'
);

CREATE VIEW person AS
SELECT
    person.id,
    person.name,
    person.emailAddress,
    person.creditCard,
    person.city,
    person.state,
    dateTime,
    person.extra
FROM nexmark WHERE event_type = 0;

CREATE VIEW auction AS
SELECT
    auction.id,
    auction.itemName,
    auction.description,
    auction.initialBid,
    auction.reserve,
    dateTime,
    auction.expires,
    auction.seller,
    auction.category,
    auction.extra
FROM nexmark WHERE event_type = 1;

CREATE VIEW bid AS
SELECT
    bid.auction,
    bid.bidder,
    bid.price,
    dateTime,
    bid.extra
FROM nexmark WHERE event_type = 2;