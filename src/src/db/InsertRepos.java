package db;

import java.net.ConnectException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.eclipse.egit.github.core.client.NoSuchPageException;
import org.eclipse.egit.github.core.client.PageIterator;
import org.eclipse.egit.github.core.client.RequestException;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.egit.github.core.Repository;

public class InsertRepos implements Runnable
{
	private final String ADD_REPO_STR = "INSERT INTO gh_repositories (id, owner_id, name, description, fork, " +
		"url, html_url) VALUES (?, ?, ?, ?, ?, ?, ?)";
	private final String EXT_REPO_STR = "SELECT COUNT(*) as cnt FROM gh_repositories WHERE id = ?";
	private final String MAX_REPO_STR = "SELECT IFNULL(MAX(id), 0) as umax FROM gh_repositories";
	private final DBConnector m_cons;

	public InsertRepos(DBConnector _m_cons)
	{
		this.m_cons = _m_cons;
	}
	
	private void createRepos() throws SQLException, InterruptedException, NoSuchPageException, ConnectException
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		PageIterator<Repository> iterator = null;
		PreparedStatement st = null;
		List<Repository> repos;

		RepositoryService uservice;
		int maxUser;
		Date date;

		try
		{
			Connection con = this.m_cons.newConn();
			maxUser = MaxUser(con);
			st = con.prepareStatement(ADD_REPO_STR);

			uservice = new RepositoryService(this.m_cons.getGHClient(con));
			iterator = uservice.pageAllRepositories(maxUser);

			while (iterator.hasNext())
			{
				date = new Date();
				repos = new ArrayList<Repository>();
				repos.addAll(iterator.next());
				
				System.out.print(repos.size() + " repos to insert ");
				System.out.print("[" + repos.get(0).getId() + " - " + repos.get(repos.size() - 1).getId() + "]");
				System.out.println(" on " + dateFormat.format(date));

				for(Repository u2 : repos)
				{
					if(this.RepoExists(con, u2.getId()))
						continue;
					else
					{
						st.setLong(1, u2.getId());

						if(u2.getOwner() != null)
							st.setInt(2, u2.getOwner().getId());
						else
							st.setNull(2, java.sql.Types.INTEGER);

						st.setString(3, u2.getName());
						st.setString(4, u2.getDescription() == null ? null :
							(u2.getDescription().length() > 511 ? u2.getDescription().substring(0, 511) : u2.getDescription()));
						st.setInt(5, u2.isFork() ? 1 : 0);
						st.setString(6, u2.getUrl());
						st.setString(7, u2.getHtmlUrl());
						st.executeUpdate();
					}
				}
			}
		}
		catch (SQLException | InterruptedException | NoSuchPageException ex)
		{
	        throw ex;
		}
		finally
		{
			if(st != null) st.close();
		}
	}

	private boolean RepoExists(Connection conn, long id) throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;

		try
		{
			st = conn.prepareStatement(EXT_REPO_STR);
			st.setLong(1, id);
			rs = st.executeQuery();

			while (rs.next())
			{
				if(rs.getInt("cnt") > 0)
					return true;
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
		
		return false;
	}

	private int MaxUser(Connection conn) throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;
		int rVal = 0;

		try
		{
			st = conn.prepareStatement(MAX_REPO_STR);
			rs = st.executeQuery();

			while (rs.next())
			{
				rVal = rs.getInt("umax");
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

	@Override
	public void run()
	{
		boolean retry = false;
		
		do
		{
			retry = false;

			try
			{
				this.createRepos();
			}
			catch (SQLException | InterruptedException e)
			{
				e.printStackTrace();
				retry = false;
			}
			catch(NoSuchPageException | ConnectException e)
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

						if(status >= 404)
						{
							TimeUnit.MINUTES.sleep(10L);
							retry = true;
						}
					}
					else if(e instanceof ConnectException)
					{
						TimeUnit.MINUTES.sleep(10L);
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