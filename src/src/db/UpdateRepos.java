package db;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.eclipse.egit.github.core.Repository;
import org.eclipse.egit.github.core.Contributor;
import org.eclipse.egit.github.core.RepositoryId;
import org.eclipse.egit.github.core.client.RequestException;
import org.eclipse.egit.github.core.service.RepositoryService;

public class UpdateRepos implements Runnable
{
	private final DBConnector m_cons;
	private final boolean justUpdating;
	private final boolean isMac;

	//REPO TABLE
	private final String NXT_RCOL_STR = "SELECT id, url FROM gh_repositories WHERE id > ? LIMIT ?";
	private final String NXT_REPO_STR = "SELECT id, url FROM gh_repositories WHERE created_at IS NULL LIMIT ?";
	private final String UPD_REPO_STR = "UPDATE gh_repositories SET created_at = ?, stargazers_count = ?, " +
			"watchers_count = ?, language = ?, forks_count = ?, open_issues_count = ?, forks = ?, " +
			"open_issues = ?, watchers = ?, network_count = ?, subscribers_count = ? WHERE id = ? LIMIT 1";
	private final String DEL_REPO_STR = "DELETE FROM gh_repositories WHERE id = ? LIMIT 1";

	//CONTRIBUTORS TABLES
	private final String INS_COLLB_STR = "INSERT IGNORE INTO gh_repos_collaborators (repo_id, user_id) VALUES (?, ?)";
	private final String INS_STARG_STR = "INSERT IGNORE INTO gh_repos_contributors (repo_id, user_id) VALUES (?, ?)";
	private final String INS_CONTR_STR = "INSERT IGNORE INTO gh_repos_star (repo_id, user_id) VALUES (?, ?)";
	private final String INS_SUBSC_STR = "INSERT IGNORE INTO gh_repos_subscribers (repo_id, user_id) VALUES (?, ?)";

	public UpdateRepos(DBConnector _m_cons, boolean updating, boolean mac)
	{
		this.m_cons = _m_cons;
		this.justUpdating = updating;
		this.isMac = mac;
	}

	private void completeRepos() throws SQLException, InterruptedException, IOException
	{
		Date date;
		PreparedStatement st = null;
		RepositoryService uservice;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		try
		{
			Connection conn = this.m_cons.newConn();
			ArrayList<String []> repos = this.getRepos(conn, 500);
			st = conn.prepareStatement(UPD_REPO_STR);
			uservice = new RepositoryService(this.m_cons.getGHClient(conn));

			do
			{
				date = new Date();
				System.out.print(repos.size() + " repos to update ");
				System.out.print("[" + repos.get(0)[0] + " - " + repos.get(repos.size() - 1)[0] + "]");
				System.out.println(" on " + dateFormat.format(date));
	
				for(String[] u : repos)
				{
					Repository u2 = uservice.getRepository(u[1], u[2]);

					st.setDate(1, new java.sql.Date(u2.getCreatedAt().getTime()));
					st.setInt(2, u2.getStargazersCount());
					st.setInt(3, u2.getWatchersCount());
					st.setString(4, u2.getLanguage());
					st.setInt(5, u2.getForksCount());
					st.setInt(6, u2.getOpenIssuesCount());
					st.setInt(7, u2.getForks());
					st.setInt(8, u2.getOpenIssues());
					st.setInt(9, u2.getWatchers());
					st.setInt(10, u2.getNetworkCount());
					st.setInt(11, u2.getSubscribersCount());
					st.setLong(12, u2.getId());
					st.executeUpdate();
				}
			
				repos = this.getRepos(conn, 500);
			}
			while(repos.size() > 0);
		}
		catch (SQLException ex)
		{
	        throw ex;
		}
		finally
		{
			if(st != null) st.close();
		}
	}
	
	private ArrayList<String []> getRepos(Connection conn, int limit) throws SQLException
	{
		ArrayList<String []> rVal = new ArrayList<String []>();
		PreparedStatement st = null;
		ResultSet rs = null;

		try
		{
			st = conn.prepareStatement(NXT_REPO_STR);
			st.setInt(1, limit);
			rs = st.executeQuery();

			while (rs.next())
			{
				rVal.add(this.getUser_RepoFromURL(rs.getString("id"), rs.getString("url")));
			}
		}
		catch (SQLException ex)
		{
	        throw ex;
		}
		finally
		{
			if(st != null) st.close();
			if(rs != null) rs.close();
		}

		return rVal;
	}

	private ArrayList<String []> getReposCollaborators(Connection conn, long id, int limit) throws SQLException
	{
		ArrayList<String []> rVal = new ArrayList<String []>();
		PreparedStatement st = null;
		ResultSet rs = null;

		try
		{
			st = conn.prepareStatement(NXT_RCOL_STR);
			st.setLong(1, id);
			st.setInt(2, limit);
			rs = st.executeQuery();

			while (rs.next())
			{
				rVal.add(this.getUser_RepoFromURL(rs.getString("id"), rs.getString("url")));
			}
		}
		catch (SQLException ex)
		{
	        throw ex;
		}
		finally
		{
			if(st != null) st.close();
			if(rs != null) rs.close();
		}

		return rVal;
	}

	private void insertAllContributors() throws SQLException, InterruptedException, IOException, ConnectException
	{
		Date date;
		PreparedStatement stCollab = null, stContrib = null, stStarg = null, stSubscrib = null;
		RepositoryService uservice;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		int collab, contrib, starg, subscrib;
		long lastId;
		
		try
		{
			Connection conn = this.m_cons.newConn();
			lastId = this.m_cons.CrawlerIntVar(conn, "lastrepo");
			ArrayList<String []> repos = this.getReposCollaborators(conn, lastId, 500);
			uservice = new RepositoryService(this.m_cons.getGHClient(conn));

			stCollab = conn.prepareStatement(INS_COLLB_STR);
			stContrib = conn.prepareStatement(INS_CONTR_STR);
			stStarg = conn.prepareStatement(INS_STARG_STR);
			stSubscrib = conn.prepareStatement(INS_SUBSC_STR);

			do
			{
				date = new Date();
				System.out.println("On " + dateFormat.format(date));
				List<Contributor> u2 = null;
				collab = contrib = starg = subscrib = 0;

				for(String[] u : repos)
				{
					lastId = Long.parseLong(u[0]);
					RepositoryId repId = new RepositoryId(u[1], u[2]);

					if(this.isMac)
					{
						//Contributors
						u2 = uservice.getContributors(repId, false);
						contrib = u2.size();
	
						for(Contributor c0nt : u2)
						{
							stContrib.setLong(1, lastId);
							stContrib.setInt(2, c0nt.getId());
							stContrib.executeUpdate();
						}
	
						//Collaborators
						u2 = uservice.getCollaborators(repId, false);
						collab = u2.size();
	
						for(Contributor c0nt : u2)
						{
							stCollab.setLong(1, lastId);
							stCollab.setInt(2, c0nt.getId());
							stCollab.executeUpdate();
						}
					}
					else
					{
						//Stargazers
						u2 = uservice.getStargazers(repId, false);
						starg = u2.size();
	
						for(Contributor c0nt : u2)
						{
							stStarg.setLong(1, lastId);
							stStarg.setInt(2, c0nt.getId());
							stStarg.executeUpdate();
						}
	
						//Subscribers
						u2 = uservice.getContributors(repId, false);
						subscrib = u2.size();
	
						for(Contributor c0nt : u2)
						{
							stSubscrib.setLong(1, lastId);
							stSubscrib.setInt(2, c0nt.getId());
							stSubscrib.executeUpdate();
						}
					}

					this.m_cons.UpdateCrawlerIntVar(conn, "lastrepo", lastId);
					System.out.print("Repo ID #" + lastId + " has [" + contrib + "] Contributors {" + collab + "} Collaborators ");
					System.out.println("(" + starg + ") Stargazers and |" + subscrib + "| Subscribers");
				}

				repos = this.getReposCollaborators(conn, lastId, 500);
			}
			while(repos.size() > 0);
		}
		catch (SQLException ex)
		{
	        throw ex;
		}
		finally
		{
			if(stCollab != null) stCollab.close();
			if(stContrib != null) stContrib.close();
			if(stStarg != null) stStarg.close();
			if(stSubscrib != null) stSubscrib.close();
		}
	}

	private String[] getUser_RepoFromURL(String id, String url)
	{
		String[] split = url.substring(url.indexOf("repos")).split("/");
		return new String[] { id, split[1], split[2] };
	}

	private void deleteRepo(Connection conn) throws SQLException
	{
		PreparedStatement st = null;
		
		try
		{
			ArrayList<String []> ar = getRepos(conn, 1);
			st = conn.prepareStatement(DEL_REPO_STR);
			st.setLong(1, Long.parseLong(ar.get(0)[0]));
			st.executeUpdate();
		}
		catch (SQLException e)
		{
			throw e;
		}
		finally
		{
			try { if(st != null) st.close(); }
			catch (SQLException e) { }
		}
	}

	@Override
	public void run()
	{
		boolean retry = false;
		
		do
		{
			retry = false;

			try
			{
				if(this.justUpdating)
					this.completeRepos();
				else
					this.insertAllContributors();
			}
			catch (SQLException | InterruptedException e)
			{
				e.printStackTrace();
				retry = false;
			}
			catch(IOException e)
			{
				e.printStackTrace();

				try
				{
					if(e.getMessage().indexOf("API rate limit exceeded") > -1)
						retry = true;
					else if(e instanceof RequestException)
					{
						RequestException re = (RequestException) e;
						int status = re.getStatus();

						if(status == 404)
						{
							this.deleteRepo(this.m_cons.newConn());
							retry = true;
						}
						else if(status > 404)
						{
							TimeUnit.MINUTES.sleep(10L);
							retry = true;
						}
					}
					else if(e instanceof ConnectException)
					{
						TimeUnit.MINUTES.sleep(3L);
						retry = true;
					}
					else if(e instanceof java.net.MalformedURLException)
					{
						this.deleteRepo(this.m_cons.newConn());
						retry = true;
					}
				}
				catch(Exception ec)
				{
					System.out.println("Deeeep catch*************************");
					e.printStackTrace();
				}
			}
		}
		while(retry);
	}
}