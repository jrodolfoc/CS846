use github;

#One time only
#delete from top100users where id > 0;
#insert into top100users (id) (select id from gh_users order by followers desc limit 100);
#delete from top1kusers where id > 0;
#insert into top1kusers (id) (select id from gh_users order by followers desc limit 1000);
#delete from top10kusers where id > 0;
#insert into top10kusers (id) (select id from gh_users order by followers desc limit 10000);

#delete from top100repos where id > 0;
#insert into top100repos (id) (select id from gh_repositories order by stargazers_count DESC Limit 100);
#delete from top1krepos where id > 0;
#insert into top1krepos (id) (select id from gh_repositories order by stargazers_count DESC Limit 1000);
#delete from top10krepos where id > 0;
#insert into top10krepos (id) (select id from gh_repositories order by stargazers_count DESC Limit 10000);

#Re-Do
delete from users_x_repos where uid > 0 AND rid > 0;
insert ignore into users_x_repos(uid, rid) (select user_id, repo_id from gh_repos_collaborators);
insert ignore into users_x_repos(uid, rid) (select user_id, repo_id from gh_repos_contributors);
insert ignore into users_x_repos(uid, rid) (select user_id, repo_id from gh_repos_subscribers);
insert ignore into users_x_repos(uid, rid) (select user_id, repo_id from gh_repos_star);

#Top 100 users
select count(distinct x.rid) as Total from users_x_repos x join top100users u ON u.id = x.uid join top100repos r on r.id = x.rid;
select count(distinct x.rid) as Total from users_x_repos x join top100users u ON u.id = x.uid join top1krepos r on r.id = x.rid;
select count(distinct x.rid) as Total from users_x_repos x join top100users u ON u.id = x.uid join top10krepos r on r.id = x.rid;

#Top 1k users
select count(distinct x.rid) as Total from users_x_repos x join top1kusers u ON u.id = x.uid join top100repos r on r.id = x.rid;
select count(distinct x.rid) as Total from users_x_repos x join top1kusers u ON u.id = x.uid join top1krepos r on r.id = x.rid;
select count(distinct x.rid) as Total from users_x_repos x join top1kusers u ON u.id = x.uid join top10krepos r on r.id = x.rid;

#Top 10k users
select count(distinct x.rid) as Total from users_x_repos x join top10kusers u ON u.id = x.uid join top100repos r on r.id = x.rid;
select count(distinct x.rid) as Total from users_x_repos x join top10kusers u ON u.id = x.uid join top1krepos r on r.id = x.rid;
select count(distinct x.rid) as Total from users_x_repos x join top10kusers u ON u.id = x.uid join top10krepos r on r.id = x.rid;