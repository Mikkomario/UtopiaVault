DROP DATABASE IF EXISTS test_db;
CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

CREATE TABLE test1
(
	id 			bigint 			NOT NULL PRIMARY KEY AUTO_INCREMENT,
	name 		varchar(32) 	NOT NULL,
	additional 	int 			NOT NULL
);