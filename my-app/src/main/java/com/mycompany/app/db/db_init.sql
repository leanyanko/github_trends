drop table if exists last_commit;
create table last_commit (id serial primary key, repo_name varchar(200), date date);

drop table if exists langs;
create table langs (id serial primary key, repo_name varchar(200), lang varchar(200), bytes bigint);