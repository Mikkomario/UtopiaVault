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

SELECT arrival_flight.af_rowId FROM arrival_flight WHERE 
(arrival_flight.af_scheduledArrivalTime BETWEEN '2016-02-28 17:25:00' AND '2016-02-29 05:25:00')


SELECT arrival_flight.af_rowId FROM arrival_flight WHERE 
(arrival_flight.af_registration <=> 'CSTNP' AND 
arrival_flight.af_scheduledArrivalTime IS BETWEEN '2016-02-28 17:25:00' AND '2016-02-29 05:25:00') ORDER BY af_scheduledArrivalTime DESC LIMIT 1


You have an error in your SQL syntax; check the manual that corresponds to your MariaDB 
server version for the right syntax to use near 'BETWEEN '2016-02-28 17:25:00' AND '2016-02-29 05:25:00') ORDER BY af_scheduledAr' at line 1