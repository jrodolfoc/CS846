package db;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.egit.github.core.client.PageIterator;
import org.eclipse.egit.github.core.service.UserService;

import org.eclipse.egit.github.core.User;

public class Users
{
	private final String ADD_USER_STR = "INSERT INTO gh_users (id, login, type) VALUES (?, ?, ?)";
	private final String UPD_USER_STR = "UPDATE gh_users SET url = ?, public_repos = ?, name = ?, company = ?, location = ?, followers = ?, following = ?, created_at = ? WHERE id = ? LIMIT 1";
	private final String NXT_USER_STR = "SELECT id, login, type FROM gh_users WHERE followers IS NULL LIMIT 500";
	private final String EXT_USER_STR = "SELECT COUNT(*) as cnt FROM gh_users WHERE id = ?";
	private final String MAX_USER_STR = "SELECT IFNULL(MAX(id), 0) as umax FROM gh_users";
	private final DBConnector m_cons;

	public Users(DBConnector _m_cons)
	{
		this.m_cons = _m_cons;
	}
	
	public void UpdateUsers() throws SQLException, InterruptedException, IOException
	{
		PreparedStatement st = null;
		ArrayList<User> users = this.getUsers();
		UserService uservice = new UserService(this.m_cons.getGHClient());

		do
		{
			for(User u : users)
			{
				User u2 = uservice.getUser(u.getLogin());
		
				try
				{
					st = this.m_cons.getConn().prepareStatement(UPD_USER_STR);
					st.setString(1, u2.getUrl());
					st.setInt(2, u2.getPublicRepos());
					st.setString(3, u2.getName());
					st.setString(4, u2.getCompany());
					st.setString(5, u2.getLocation());
					st.setInt(6, u2.getFollowers());
					st.setInt(7, u2.getFollowing());
					st.setDate(8, (Date) u2.getCreatedAt());
					st.setInt(9, u2.getId());
					st.executeUpdate();
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
		
			users = this.getUsers();
		}
		while(users.size() > 0);
	}

	public void InsertUsers() throws SQLException, InterruptedException, IOException
	{
		Map<String, String> map = new HashMap<String, String>();
		PageIterator<User> iterator = null;
		Collection<User> users = null;
		PreparedStatement st = null;

		UserService uservice;
		int maxUser;

		try
		{
			maxUser = MaxUser();
			map.put("since", "" + maxUser);
			st = this.m_cons.getConn().prepareStatement(ADD_USER_STR);

			uservice = new UserService(this.m_cons.getGHClient());
			iterator = uservice.pageUsers(map);

			while (iterator.hasNext())
			{	
				users = iterator.next();

				for(User u2 : users)
				{
					if(this.UserExists(u2.getId()))
						continue;
					else
					{
						st.setInt(1, u2.getId());
						st.setString(2, u2.getLogin());
						st.setString(3, u2.getType());
						st.executeUpdate();
						System.out.println("Inserted User ID: " + u2.getId());
					}
				}
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
		}
	}

	private boolean UserExists(int id) throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;

		try
		{
			st = this.m_cons.getConn().prepareStatement(EXT_USER_STR);
			st.setInt(1, id);
			rs = st.executeQuery();

			while (rs.next())
			{
				if(rs.getInt("cnt") > 0)
					return true;
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
		
		return false;
	}

	private int MaxUser() throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;
		int rVal = 0;

		try
		{
			st = this.m_cons.getConn().prepareStatement(MAX_USER_STR);
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
	
	private ArrayList<User> getUsers() throws SQLException
	{
		ArrayList<User> rVal = new ArrayList<User>();
		PreparedStatement st = null;
		ResultSet rs = null;
		User u = null;

		try
		{
			st = this.m_cons.getConn().prepareStatement(NXT_USER_STR);
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
}