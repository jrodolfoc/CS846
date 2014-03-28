package db;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.eclipse.egit.github.core.Repository;
import org.eclipse.egit.github.core.client.RequestException;
import org.eclipse.egit.github.core.service.RepositoryService;

public class UpdateRepos implements Runnable
{
	private final String UPD_REPO_STR = "UPDATE gh_repositories SET created_at = ?, stargazers_count = ?, " +
			"watchers_count = ?, language = ?, forks_count = ?, open_issues_count = ?, forks = ?, " +
			"open_issues = ?, watchers = ?, network_count = ?, subscribers_count = ? WHERE id = ? LIMIT 1";
	private final String NXT_REPO_STR = "SELECT id, url FROM gh_repositories WHERE created_at IS NULL LIMIT ?";
	private final String DEL_REPO_STR = "DELETE FROM gh_repositories WHERE id = ? LIMIT 1";
	private final DBConnector m_cons;
	
	public UpdateRepos(DBConnector _m_cons)
	{
		this.m_cons = _m_cons;
	}

	private void completeRepos() throws SQLException, InterruptedException, IOException
	{
		Date date;
		PreparedStatement st = null;
		RepositoryService uservice = new RepositoryService(this.m_cons.getGHClient());
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		try
		{
			Connection conn = this.m_cons.newConn();
			ArrayList<String []> repos = this.getRepos(conn, 500);
			st = conn.prepareStatement(UPD_REPO_STR);

			do
			{
				date = new Date();
				System.out.print(repos.size() + " users to update ");
				System.out.print("[" + repos.get(0)[0] + " - " + repos.get(repos.size() - 1)[0] + "]");
				System.out.println(" on " + dateFormat.format(date));
	
				for(String[] u : repos)
				{
//"UPDATE gh_repositories SET created_at = ?, stargazers_count = ?, " +
//"watchers_count = ?, language = ?, forks_count = ?, open_issues_count = ?, forks = ?, " +
//"open_issues = ?, watchers = ?, network_count = ?, subscribers_count = ?, classification = ? " +
//"WHERE id = ? LIMIT 1";
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
			System.out.println(ex.getMessage());
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
		String[] u = null;

		try
		{
			st = conn.prepareStatement(NXT_REPO_STR);
			st.setInt(1, limit);
			rs = st.executeQuery();

			while (rs.next())
			{
				String url = rs.getString("url");
				String[] split = url.substring(url.indexOf("repos")).split("/");

				u = new String[] { rs.getString("id"), split[1], split[2] };
				rVal.add(u);
			}
		}
		catch (SQLException ex)
		{
			System.out.println(ex.getMessage());
	        throw ex;
		}
		finally
		{
			if(st != null) st.close();
			if(rs != null) rs.close();
		}

		return rVal;
	}

	private void deleteRepo(Connection conn)
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
			e.printStackTrace();
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
			try
			{
				this.completeRepos();
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
					if(e instanceof RequestException)
					{
						RequestException re = (RequestException) e;

						if(re.getStatus() == 404)
						{
							this.deleteRepo(this.m_cons.newConn());
							retry = true;
						}
						else
						{
							TimeUnit.MINUTES.sleep(5L);
							retry = true;
						}
					}
					else
						retry = false;
				}
				catch(Exception ec) { }
			}
		}
		while(retry);
	}
}