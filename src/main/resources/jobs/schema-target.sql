create table target_profile
(user_id varchar(50) not null,
profile_id varchar(50) not null
);

/*hsql needs a table dual to be created, in oracle dual is already there */
create table dual (dummy_col varchar(1));
insert into dual values('X');