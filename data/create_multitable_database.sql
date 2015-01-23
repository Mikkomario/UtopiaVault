/* This database is used for storing information about the multi table system */
DROP DATABASE IF EXISTS multitable_db;
CREATE DATABASE IF NOT EXISTS multitable_db;

USE multitable_db;

CREATE TABLE tableamounts
(
	tableName 	varchar(32) 	NOT NULL PRIMARY KEY,
	latestIndex int 			NOT NULL
);