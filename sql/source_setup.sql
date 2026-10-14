CREATE DATABASE IF NOT EXISTS grammy_source;
USE grammy_source;
CREATE TABLE grammy_awards (year INT, title VARCHAR(255), published_at VARCHAR(255), updated_at VARCHAR(255), category VARCHAR(255), nominee VARCHAR(255), artist VARCHAR(255), workers TEXT, img TEXT, winner VARCHAR(10));
LOAD DATA INFILE '/var/lib/mysql-files/grammy.csv' INTO TABLE grammy_awards FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;