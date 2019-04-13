CREATE STREAM masterStream (content VARCHAR, sentiment VARCHAR, profanity VARCHAR, personal VARCHAR)
  WITH (KAFKA_TOPIC='content_curator_twitter', VALUE_FORMAT='JSON') ;

CREATE STREAM content
  WITH(VALUE_FORMAT='JSON')
  AS SELECT ROWKEY as uuid, content
  FROM masterStream
  WHERE content IS NOT NULL ;


CREATE STREAM sentiment
  WITH(VALUE_FORMAT='JSON')
  AS SELECT ROWKEY as uuid, sentiment
  FROM masterStream
  WHERE sentiment IS NOT NULL ;


CREATE STREAM profanity
  WITH(VALUE_FORMAT='JSON')
  AS SELECT ROWKEY as uuid, profanity
  FROM masterStream
  WHERE profanity IS NOT NULL ;


CREATE STREAM personal
  WITH(VALUE_FORMAT='JSON')
  AS SELECT ROWKEY as uuid, personal
  FROM masterStream
  WHERE personal IS NOT NULL ;


CREATE TABLE contentTable (uuid VARCHAR, content VARCHAR)
  WITH (KAFKA_TOPIC='CONTENT', VALUE_FORMAT='JSON', KEY='uuid') ;

CREATE TABLE sentimentTable (uuid VARCHAR, sentiment VARCHAR)
  WITH (KAFKA_TOPIC='SENTIMENT', VALUE_FORMAT='JSON', KEY='uuid') ;

CREATE TABLE profanityTable (uuid VARCHAR, profanity VARCHAR)
  WITH (KAFKA_TOPIC='PROFANITY', VALUE_FORMAT='JSON', KEY='uuid') ;

CREATE TABLE personalTable (uuid VARCHAR, personal VARCHAR)
  WITH (KAFKA_TOPIC='PERSONAL', VALUE_FORMAT='JSON', KEY='uuid') ;


CREATE TABLE preView1
  WITH(VALUE_FORMAT='JSON')
  AS SELECT IFNULL(c.uuid, s.uuid) AS uuid, c.content AS content, s.sentiment AS sentiment
    FROM contentTable c
    FULL JOIN sentimentTable s
    ON c.uuid = s.uuid ;


CREATE TABLE preView1_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR)
  WITH (KAFKA_TOPIC='PREVIEW1', VALUE_FORMAT='JSON', KEY='UUID') ;


CREATE TABLE preView2
  WITH(VALUE_FORMAT='JSON')
  AS SELECT IFNULL(pv1.uuid, pro.uuid) AS uuid, pv1.content AS content,  pv1.sentiment AS sentiment, pro.profanity AS profanity
    FROM preView1_t pv1
    FULL JOIN profanityTable pro
    ON pv1.uuid = pro.uuid ;


CREATE TABLE preView2_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR, profanity VARCHAR)
  WITH (KAFKA_TOPIC='PREVIEW2', VALUE_FORMAT='JSON', KEY='UUID') ;


CREATE TABLE preView3
  WITH(VALUE_FORMAT='JSON')
  AS SELECT IFNULL(pv2.uuid, pro.uuid) AS uuid, pv2.content AS content,  pv2.sentiment AS sentiment, pv2.profanity AS profanity, pro.personal AS personal
    FROM preView2_t pv2
    FULL JOIN personalTable pro
    ON pv2.uuid = pro.uuid ;


CREATE TABLE preView3_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR, profanity VARCHAR, personal VARCHAR)
  WITH (KAFKA_TOPIC='PREVIEW3', VALUE_FORMAT='JSON', KEY='UUID') ;