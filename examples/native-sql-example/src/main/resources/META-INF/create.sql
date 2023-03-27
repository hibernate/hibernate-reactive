create sequence author_SEQ start 1 increment 50
create sequence book_SEQ start 1 increment 50
create table authors (id int4 not null, name varchar(100) not null, primary key (id))
create table books (id int4 not null, isbn varchar(13), published date not null, title varchar(100) not null, author_id int4 not null, primary key (id), foreign key (author_id) references authors)
