drop table if exists all_commits;
create table all_commits (id serial primary key, repo_name varchar(200), date date);

drop table if exists langs;
create table langs (id serial primary key, repo_name varchar(200), lang varchar(200), bytes bigint);

drop table if exists fileids;
create table fileids (id serial primary key, repo_name varchar(200), fileid varchar (500));


create table ids_lang as select ids.id, ids.repo_name, langs.lang, bytes left join langs on (langs.repo_name = ids.repo_name);

CREATE TABLE new_records AS
SELECT t.* FROM new_table t JOIN new_record_ids r ON(r.id = t.id);

create table tmp_lan_commit as select langs.repo_name, langs.lang, langs.bytes, last_commit.date from langs join last_commit on (langs.repo_name = last_commit.repo_name);
delete from last_commit_new a using last_commit_new b where a.date < b.date and a.repo_name = b.repo_name;

--#find duplicates
select repo_name, count(repo_name) from last_commit_new group by repo_name having count(repo_name)>1 limit 10;

--#are there any duplicates?
select repo_name, count(repo_name) from tmp_cleaned group by repo_name having count(repo_name)>1 limit 20;

--#export remote table to local csv

\copy tmp_cleaned to '/Users/annaleonenko/last_commit.csv' delimiter ',' csv header;



create table each_lang_per_day as select lang, date, count(date) from lan_to_commit group by lang, date;

create table commits_per_day as select date, count(date) from lan_to_commit  group by date;


--create table commits_per_month as SELECT YEAR(date) as year_c, MONTH(date) as month_c, sum(count) AS total FROM commits_per_day
--GROUP BY YEAR(date), MONTH(date)
--ORDER BY YEAR(date), MONTH(date);


create table commits_per_month as select to_char(date,'Mon') as mon,
       extract(year from date) as yyyy,
       sum("count") as total
from commits_per_day
group by 1,2

create table each_lan_per_month as
select lang,
       to_char(date,'Mon') as mon,
       extract(year from date) as yyyy,
       sum("count") as total
from each_lang_per_day
group by 1,2,3

--calculate percents for each langs from total for each month
create table rel_each_lan as
    select each_lan_per_month.lang,
           each_lan_per_month.mon,
           each_lan_per_month.YYYY,
           each_lan_per_month.total as per_lang,
           commits_per_month.total as total,
           (each_lan_per_month.total / commits_per_month.total) * 100 as percents
           from each_lan_per_month join commits_per_month on
                (each_lan_per_month.YYYY = commits_per_month.YYYY and each_lan_per_month.mon = commits_per_month.mon);

-- convert Mon and YYYY back to date
create table percents_per_date as
select
    concat(mon, ', ', YYYY) as d,
    to_date(d, 'Mon, YYYY') as date,
    lang,
    per_lang,
    total,
    percents
    from rel_each_lan;

-- convert Mon and YYYY back to date
create table percents_per_date as
select
    to_date(concat(mon, ', ', YYYY), 'Mon, YYYY') as date,
    lang,
    per_lang,
    total,
    percents
    from rel_each_lan;


-- download file
\copy percents_per_date to '/Users/annaleonenko/percents_per_month.csv' delimiter ',' csv header;


--INCLUDINS SIZE

--new
create table lan_to_commit_b as select langs.lang, langs.repo_name, langs.bytes, last_commit_new.date from langs inner join last_commit_new on (langs.repo_name = last_commit_new.repo_name);

-- PER DAY
create table each_lang_per_day_b as select lang, date, count(date), sum(bytes) as bytes from lan_to_commit group by lang, date;

create table commits_per_day_b as select date, count(date), sum(bytes) as bytes from lan_to_commit  group by date;

-- PER MONTH

create table each_lan_per_month_b as
select lang,
       to_char(date,'Mon') as mon,
       extract(year from date) as yyyy,
       sum("count") as total,
       sum(bytes) as bytes
from each_lang_per_day_b
group by 1,2,3;

--calculate percents for each langs from total for each month
create table rel_each_lan_b as
    select each_lan_per_month_b.lang,
           each_lan_per_month_b.bytes,
           each_lan_per_month_b.mon,
           each_lan_per_month_b.YYYY,
           each_lan_per_month_b.total as per_lang,
           commits_per_month.total as total,
           (each_lan_per_month_b.total / commits_per_month.total) * 100 as percents
           from each_lan_per_month_b join commits_per_month on
                (each_lan_per_month_b.YYYY = commits_per_month.YYYY and each_lan_per_month_b.mon = commits_per_month.mon);

-- convert Mon and YYYY back to date
create table percents_per_date_b as
select
    to_date(concat(mon, ', ', YYYY), 'Mon, YYYY') as date,
    lang,
    bytes,
    per_lang,
    total,
    percents
    from rel_each_lan_b;

\copy percents_per_date_b to '/Users/annaleonenko/percents_per_month_b.csv' delimiter ',' csv header;