USE test_db;

CREATE TABLE roles
(
	id int primary key not null,
	name varchar(64) not null
)Engine=InnoDB;

CREATE TABLE users
(
	id int primary key not null auto_increment,
	name varchar(64) not null,
	role_id int not null,
	FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE
)Engine=InnoDB;


/*
SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE REFERENCED_TABLE_NAME = 'roles';

SELECT TABLE_NAME,COLUMN_NAME,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE REFERENCED_TABLE_NAME = 'roles' AND REFERENCED_TABLE_SCHEMA = 'test_db';
*/
-- TODO: ASC ja DESC orderByhyn
-- TODO: Like / starts with condition