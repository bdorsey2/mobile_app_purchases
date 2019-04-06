
--------------------------------EVENTS-----------------------------------------------------
create table events_full (
app_id bigint
, session_id bigint
, event varchar(50)
, event_timestamp bigint
, event_value float
, user_id_hash varchar(256));

delete from events_full e
where not exists(select distinct user_id_hash from sessions s where s.user_id_hash = e.user_id_hash);

CREATE INDEX events_user_id_idx
ON events_full (user_id_hash);

ALTER TABLE events_full 
DROP COLUMN app_id;

ALTER TABLE events_full
ALTER COLUMN user_id_hash TYPE varchar(64);

select count(*) from events_full;
select session

create table user_id_map
as
select distinct user_id_hash, row_number() over () from events_full;

select distinct app_id from sessions;
-- delete from events;
-- drop table events;

create table events (
app_id bigint
, session_id bigint
, event varchar(50)
, event_timestamp bigint
, event_value float
, user_id_hash varchar(256));

CREATE INDEX user_id_idx
ON events (user_id_hash);

--\copy events from 'events.csv' with DELIMITER ',' CSV HEADER; --runs in psql cli tool


select count(*) from events;

--ALTER TABLE events
--ALTER COLUMN event_timestamp TYPE DATE USING to_timestamp(event_timestamp / 1000)::date;

--------------------------------SESSIONS-----------------------------------------------------
drop table sessions;
create table sessions (
app_id bigint
, session_id bigint
, start_timestamp bigint
, timezone varchar(50)
, timezone_offset int
, previous_sessions_duration int
, user_created_timestamp bigint
, is_user_first_session boolean
, is_session boolean
, is_developer boolean
, is_wau boolean
, is_mau boolean
, country varchar(10)
, region varchar(10)
, city varchar(50)
, latitude float
, longitude float
, locale varchar(50)
, os_name varchar(25)
, session_index int
, device_id varchar(256)
, user_id_hash varchar(256));

ALTER TABLE sessions 
DROP COLUMN app_id;

CREATE INDEX sess_user_id_idx
ON sessions (user_id_hash);

CREATE INDEX sess_id_idx
ON sessions (session_id);

--\copy sessions from 'sessions.csv' with DELIMITER ',' CSV HEADER; --runs in psql cli tool

select start_timestamp + timezone_offset *  interval '1 millisecond' from sessions limit 10;

ALTER TABLE sessions
ADD COLUMN timestamp_tz_adj TIMESTAMP;

UPDATE sessions
SET timestamp_tz_adj=to_timestamp((start_timestamp + timezone_offset) / 1000); -- *  interval '1 millisecond';

ALTER TABLE sessions
ADD COLUMN user_created_timestamp_adj TIMESTAMP;

UPDATE sessions
SET user_created_timestamp_adj=to_timestamp(user_created_timestamp / 1000); -- *  interval '1 millisecond';

ALTER TABLE sessions
ALTER COLUMN start_timestamp TYPE TIMESTAMP USING to_timestamp(start_timestamp / 1000),
ALTER COLUMN user_created_timestamp TYPE TIMESTAMP USING to_timestamp(user_created_timestamp / 1000);

select * from sessions limit 50;
-----------------------------------MESSAGES---------------------------------------------------------

--\copy messages from 'messages.csv' with DELIMITER ',' CSV HEADER; --runs in psql cli tool
drop table messages;
create table messages (
app_id bigint
, message_id bigint
, action_type varchar(50)
, delivery_type int
, delivery_time_mode int
, goal_kind int);

select * from messages where app_id=4724682771660800;

--------------------------------------SAMPLE SUB----------------------------------------------------------

-- \copy sample_submission from 'sample_submission_2.csv' with DELIMITER ',' CSV HEADER;
drop table sample_submission;
create table sample_submission (
user_id_hash varchar(256)
, user_purchase_binary_7_days float
, user_purchase_binary_14_days float);

CREATE INDEX smpl_user_id_idx
ON sample_submission (user_id_hash);

------------------------USER ID Queries--------------------------------------------------------------

select distinct user_id_hash from events;
select distinct user_id_hash from sessions;
select distinct user_id_hash from sample_submission;

select user_id_hash
from 
(select distinct user_id_hash 
from sessions) as A
left join
(select distinct user_id_hash
from events) as B using (user_id_hash);

select user_id_hash
from 
(select distinct user_id_hash 
from sessions) as A
left join
(select distinct user_id_hash
from sample_submission) as B using (user_id_hash);

------------------------------Misc. Exploration Queries---------------------------------------------

select distinct app_id from events;
select * from events e inner join sessions s using (user_id_hash);
select user_id_hash, count(distinct session_id) from sessions group by user_id_hash;



select start_timestamp, timezone_offset, timestamp_tz_adj from sessions limit 5;

select distinct event from events;

select max(message_id) - min(message_id) from messages;

select distinct(message_id) from messages;



-----------------Features and Labels Tables----------------------------------------------

--7 days train labels
drop table train_labels_table;
create table train_labels_table
as 
select
user_id_hash,
CASE
    WHEN purchases is not null THEN 1
    ELSE 0 
END as target
from 
(select distinct user_id_hash 
from sessions) as A
left join
(select 
user_id_hash,
count(event_value) as purchases
from events
where 
event_timestamp >= '2018-12-08' and event_timestamp <= '2018-12-14' and
event = '8'
group by user_id_hash) as B
using (user_id_hash);

--7 daya training features: used_days_table
drop table used_days_table;
create table used_days_table
as
select distinct on (user_id_hash)
user_id_hash,
date_part('day',('2018-12-07' - to_timestamp(user_created_timestamp/1000))) as used_days_7_train
from sessions
order by user_id_hash, used_days_7_train desc;

--7 days training features: session_number, session_num_per_day, used_days_7_train, 
-- purchases_num, purchases_num_per_day, session_duration_per_session
drop table train_features_table;
create table train_features_table
as 
select
user_id_hash,
CASE
    WHEN session_num is not null THEN session_num
    ELSE 0 
END as session_num,
used_days_7_train,
session_num/used_days_7_train as session_num_per_day,
CASE
    WHEN session_duration_per_session is not null THEN session_duration_per_session
    ELSE 0 
END as session_duration_per_session,
purchases_num,
purchases_num/used_days_7_train as purchases_num_per_day
from 
(
    select distinct user_id_hash 
    from sessions
) as A
left join
(
    select 
    user_id_hash, used_days_7_train 
    from used_days_table
) as C
using (user_id_hash)
left join
(
    select 
    user_id_hash, 
    count(distinct session_id) as session_num,
    avg(previous_sessions_duration) as session_duration_per_session
    from
    sessions
    where to_timestamp(start_timestamp/1000)<='2018-12-07' 
    and is_wau is false and is_mau is false
    and is_session is true and is_developer is false
    group by user_id_hash
) as B
using (user_id_hash)
left join
(
    select
    user_id_hash,
    CASE
        WHEN purchases_num is not null THEN purchases_num
        ELSE 0 
    END as purchases_num
    from 
    (select distinct user_id_hash 
    from sessions) as A
    left join
    (select 
    user_id_hash,
    count(event_value) as purchases_num
    from events
    where 
    event_timestamp <= '2018-12-07' and
    event = '8'
    group by user_id_hash) as B
    using (user_id_hash)
) as D
using (user_id_hash)
left join
train_labels_table
using (user_id_hash)
;
-----join features and labels
select * 
from train_features_table
left join
train_labels_table
using (user_id_hash)
limit 100;
