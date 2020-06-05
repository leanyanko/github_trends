drop table if exists year_to_extensions cascade;
create table year_to_extensions (
	id serial primary key,
	year varchar(4) not null,
	extension varchar(20) not null,
	quantity int not null);