# Aws Lambda Redshift, NodeJs Native Example


This example using Employee table

    create table if not exists Employee (
      id integer IDENTITY(1,1),
      name varchar(100) not null,
      dob varchar(100) not null,
      primary key(id));
