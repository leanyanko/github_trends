drop table if exists last_commit;
create table last_commit (id serial primary key, repo_name varchar(100), date date);