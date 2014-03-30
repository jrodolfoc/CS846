select 'id', 'owner_id', 'name', 'description', 'fork', 'created_at', 'stargazers_count', 'watchers_count', 'language',
'forks_count', 'open_issues_count',
'forks', 'open_issues', 'watchers', 'network_count', 'subscribers_count'
UNION ALL 
select id, owner_id, name, REPLACE(REPLACE(description,'"', ''),'\n', ''), fork, created_at,
stargazers_count, watchers_count, language, forks_count, open_issues_count,
forks, open_issues, watchers, network_count, subscribers_count FROM top10k_repositories

INTO OUTFILE '~/Desktop/top10k.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
