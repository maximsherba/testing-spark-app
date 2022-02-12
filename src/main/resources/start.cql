create keyspace public with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};


create table public.click("userId" bigint primary key, "timestamp" text, "page" text, "duration" int);
drop table public.click;
create table public.click("userId" int primary key, "page" text, "duration" int);



select * from public.click;

