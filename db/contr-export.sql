select 'rid', 'cnt'
UNION ALL 
select rid, count(distinct x.uid) as cnt from users_x_repos x
	left join top10kusers u ON u.id = x.uid join top10krepos r on r.id = x.rid group by rid

INTO OUTFILE '~/Desktop/contrib.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
