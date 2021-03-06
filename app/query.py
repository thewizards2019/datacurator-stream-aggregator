"""
modularise ksql queries into functions that return a single query as a string
"""

def query_list():
    """
    returns a list of individual queries to be posted to ksql server via api
    """
    q = [
        "CREATE STREAM masterStream (content VARCHAR, sentiment VARCHAR, profanity VARCHAR, personal VARCHAR, preference VARCHAR) WITH (KAFKA_TOPIC='content_curator_twitter', VALUE_FORMAT='JSON') ;",
        "CREATE STREAM content WITH(VALUE_FORMAT='JSON') AS SELECT ROWKEY as uuid, content FROM masterStream WHERE content IS NOT NULL ;",
        "CREATE STREAM sentiment WITH(VALUE_FORMAT='JSON') AS SELECT ROWKEY as uuid, sentiment FROM masterStream WHERE sentiment IS NOT NULL ;",
        "CREATE STREAM profanity WITH(VALUE_FORMAT='JSON') AS SELECT ROWKEY as uuid, profanity FROM masterStream WHERE profanity IS NOT NULL ;",
        "CREATE STREAM personal WITH(VALUE_FORMAT='JSON') AS SELECT ROWKEY as uuid, personal FROM masterStream WHERE personal IS NOT NULL ;",
        "CREATE STREAM preference WITH(VALUE_FORMAT='JSON') AS SELECT ROWKEY as uuid, preference FROM masterStream WHERE preference IS NOT NULL ;",
        "CREATE TABLE contentTable (uuid VARCHAR, content VARCHAR) WITH (KAFKA_TOPIC='CONTENT', VALUE_FORMAT='JSON', KEY='uuid') ;",
        "CREATE TABLE sentimentTable (uuid VARCHAR, sentiment VARCHAR) WITH (KAFKA_TOPIC='SENTIMENT', VALUE_FORMAT='JSON', KEY='uuid') ;",
        "CREATE TABLE profanityTable (uuid VARCHAR, profanity VARCHAR) WITH (KAFKA_TOPIC='PROFANITY', VALUE_FORMAT='JSON', KEY='uuid') ;",
        "CREATE TABLE personalTable (uuid VARCHAR, personal VARCHAR) WITH (KAFKA_TOPIC='PERSONAL', VALUE_FORMAT='JSON', KEY='uuid') ;",
        "CREATE TABLE preferenceTable (uuid VARCHAR, preference VARCHAR) WITH (KAFKA_TOPIC='PREFERENCE', VALUE_FORMAT='JSON', KEY='uuid') ;",
        "CREATE TABLE preView1 WITH(VALUE_FORMAT='JSON') AS SELECT IFNULL(c.uuid, s.uuid) AS uuid, c.content AS content, s.sentiment AS sentiment FROM contentTable c FULL JOIN sentimentTable s ON c.uuid = s.uuid ;",
        "CREATE TABLE preView1_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR) WITH (KAFKA_TOPIC='PREVIEW1', VALUE_FORMAT='JSON', KEY='UUID') ;",
        "CREATE TABLE preView2 WITH(VALUE_FORMAT='JSON') AS SELECT IFNULL(pv1.uuid, pro.uuid) AS uuid, pv1.content AS content,  pv1.sentiment AS sentiment, pro.profanity AS profanity FROM preView1_t pv1 FULL JOIN profanityTable pro ON pv1.uuid = pro.uuid ;",
        "CREATE TABLE preView2_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR, profanity VARCHAR) WITH (KAFKA_TOPIC='PREVIEW2', VALUE_FORMAT='JSON', KEY='UUID') ;",
        "CREATE TABLE preView3 WITH(VALUE_FORMAT='JSON') AS SELECT IFNULL(pv2.uuid, pro.uuid) AS uuid, pv2.content AS content,  pv2.sentiment AS sentiment, pv2.profanity AS profanity, pro.personal AS personal FROM preView2_t pv2 FULL JOIN personalTable pro ON pv2.uuid = pro.uuid ;",
        "CREATE TABLE preView3_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR, profanity VARCHAR, personal VARCHAR) WITH (KAFKA_TOPIC='PREVIEW3', VALUE_FORMAT='JSON', KEY='UUID') ;",
        "CREATE TABLE preView4 WITH(VALUE_FORMAT='JSON') AS SELECT IFNULL(pv3.uuid, p.uuid) AS uuid, pv3.content AS content, pv3.sentiment AS sentiment, pv3.profanity AS profanity, pv3.personal AS personal, p.preference AS preference FROM preView3_t pv3 FULL JOIN preferenceTable p ON pv3.uuid = p.uuid ;",
        "CREATE TABLE preView4_t (uuid VARCHAR, content VARCHAR, sentiment VARCHAR, profanity VARCHAR, personal VARCHAR, preference VARCHAR) WITH (KAFKA_TOPIC='PREVIEW4', VALUE_FORMAT='JSON', KEY='uuid') ;",
        "CREATE TABLE PREFERENCES WITH(VALUE_FORMAT='JSON') AS SELECT uuid, content, preference FROM preView4 WHERE content IS NOT NULL AND preference IS NOT NULL ;",
        "CREATE TABLE preference_t (uuid VARCHAR, content VARCHAR, preference VARCHAR) WITH (KAFKA_TOPIC='PREFERENCES', VALUE_FORMAT='JSON', KEY='uuid') ;"
    ]
    return q