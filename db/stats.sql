use github;
#VARS
select * from cwl_vars;

#REPOS
select * from gh_repositories order by stargazers_count desc limit 100;
select count(*) from gh_repositories; #1,664,376; 1,973,058; 1.972.964
select * from gh_repositories where created_at IS NOT NULL LIMIT 20;
select * from gh_repositories where created_at IS NULL LIMIT 20;
select count(*) from gh_repositories where created_at IS NOT NULL;
#482.496 6.00pm
#534,202 7.50pm
#598,358 9.16pm
#633,604 10.11pm
#868,177 11.10am
select count(*) from gh_repositories where created_at IS NULL;

#insert into cwl_vars (description, var_int_value, var_str_value) VALUES ('ubu8auth', NULL, '485fbbbdcaded56bf153553a132861f1ae68b608');

#USERS
select count(*) from gh_users;
select * from gh_users where login = '';
select * from gh_users order by rand() limit 10;
#618,254
#618,339
#618,353
select count(*) from gh_users where following is not null;
#474,687 10.49am
#509,713 11.47am
#551,796 12.53pm
select count(*) from gh_users where following is null;
select * from gh_users where following is null;

#CONTRIBUTORS
SELECT count(distinct repo_id) FROM gh_repos_subscribers;
#1,658 11.20am