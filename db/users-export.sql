select * from gh_users
INTO OUTFILE '~/Desktop/file.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
