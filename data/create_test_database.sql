DROP DATABASE IF EXISTS test_db;
CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

CREATE TABLE test
(
	test_id 			bigint 			NOT NULL PRIMARY KEY AUTO_INCREMENT,
	test_name 			varchar(32) 	NOT NULL,
	test_additional 	int 			NOT NULL default 1
)Engine=InnoDB;