-- Import file for testing schema validation in SchemaValidationTest
drop table if exists Fruit cascade constraints
drop sequence if exists Fruit_SEQ

-- Create the table manually, so that we can check if the validation succeeds
create sequence fruit_seq start with 1 increment by 50;
create table Fruit (id number(10,0) not null, something_name nvarchar2(20) not null, primary key (id))

INSERT INTO fruit(id, something_name) VALUES (1, 'Cherry');
INSERT INTO fruit(id, something_name) VALUES (2, 'Apple');
INSERT INTO fruit(id, something_name) VALUES (3, 'Banana');
ALTER SEQUENCE fruit_seq RESTART start with 4;