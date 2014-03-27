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

import org.eclipse.egit.github.core.User;
import org.eclipse.egit.github.core.client.RequestException;
import org.eclipse.egit.github.core.service.UserService;

public class UpdateUsers implements Runnable
{
	private final String UPD_USER_STR = "UPDATE gh_users SET url = ?, public_repos = ?, name = ?, company = ?, location = ?, followers = ?, following = ?, created_at = ? WHERE id = ? LIMIT 1";
	private final String NXT_USER_STR = "SELECT id, login, type FROM gh_users WHERE followers IS NULL LIMIT ?";
	private final String DEL_USER_STR = "DELETE FROM gh_users WHERE id = ? LIMIT 1";
	private final DBConnector m_cons;

	public UpdateUsers(DBConnector _m_cons)
	{
		this.m_cons = _m_cons;
	}

	private void completeUsers() throws SQLException, InterruptedException, IOException
	{
		Date date;
		PreparedStatement st = null;
		UserService uservice = new UserService(this.m_cons.getGHClient());
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		try
		{
			Connection conn = this.m_cons.newConn();
			ArrayList<User> users = this.getUsers(conn, 500);
			st = conn.prepareStatement(UPD_USER_STR);

			do
			{
				date = new Date();
				System.out.print(users.size() + " users to update ");
				System.out.print("[" + users.get(0).getId() + " - " + users.get(users.size() - 1).getId() + "]");
				System.out.println(" on " + dateFormat.format(date));
	
				for(User u : users)
				{
					User u2 = uservice.getUser(u.getLogin());
	
					st.setString(1, u2.getUrl());
					st.setInt(2, u2.getPublicRepos());
					st.setString(3, u2.getName());
					st.setString(4, u2.getCompany());
					st.setString(5, u2.getLocation());
					st.setInt(6, u2.getFollowers());
					st.setInt(7, u2.getFollowing());
					st.setDate(8, new java.sql.Date(u2.getCreatedAt().getTime()));
					st.setInt(9, u2.getId());
					st.executeUpdate();
				}
			
				users = this.getUsers(conn, 500);
			}
			while(users.size() > 0);
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
	
	private ArrayList<User> getUsers(Connection conn, int limit) throws SQLException
	{
		ArrayList<User> rVal = new ArrayList<User>();
		PreparedStatement st = null;
		ResultSet rs = null;
		User u = null;

		try
		{
			st = conn.prepareStatement(NXT_USER_STR);
			st.setInt(1, limit);
			rs = st.executeQuery();

			while (rs.next())
			{
				u = new User();
				u.setId(rs.getInt("id"));
				u.setLogin(rs.getString("login"));
				u.setType(rs.getString("type"));

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

	private void deleteUser(Connection conn)
	{
		PreparedStatement st = null;
		
		try
		{
			ArrayList<User> ar = getUsers(conn, 1);
			st = conn.prepareStatement(DEL_USER_STR);
			st.setInt(1, ar.get(0).getId());
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
				this.completeUsers();
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
							deleteUser(this.m_cons.newConn());
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