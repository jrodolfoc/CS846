package db;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.eclipse.egit.github.core.client.PageIterator;
import org.eclipse.egit.github.core.client.RequestException;
import org.eclipse.egit.github.core.service.UserService;

import org.eclipse.egit.github.core.User;

public class InsertUsers implements Runnable
{
	private final String ADD_USER_STR = "INSERT INTO gh_users (id, login, type) VALUES (?, ?, ?)";
	private final String EXT_USER_STR = "SELECT COUNT(*) as cnt FROM gh_users WHERE id = ?";
	private final String MAX_USER_STR = "SELECT IFNULL(MAX(id), 0) as umax FROM gh_users";
	private final DBConnector m_cons;

	public InsertUsers(DBConnector _m_cons)
	{
		this.m_cons = _m_cons;
	}
	
	private void createUsers() throws SQLException, InterruptedException, IOException
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Map<String, String> map = new HashMap<String, String>();
		PageIterator<User> iterator = null;
		PreparedStatement st = null;
		List<User> users;

		UserService uservice;
		int maxUser;
		Date date;

		try
		{
			Connection con = this.m_cons.newConn();
			maxUser = MaxUser(con);
			map.put("since", "" + maxUser);
			st = con.prepareStatement(ADD_USER_STR);

			uservice = new UserService(this.m_cons.getGHClient());
			iterator = uservice.pageUsers(map);

			while (iterator.hasNext())
			{
				date = new Date();
				users = new ArrayList<User>();
				users.addAll(iterator.next());
				
				System.out.print(users.size() + " users to insert ");
				System.out.print("[" + users.get(0).getId() + " - " + users.get(users.size() - 1).getId() + "]");
				System.out.println(" on " + dateFormat.format(date));

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
			st = this.m_cons.newConn().prepareStatement(EXT_USER_STR);
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

	private int MaxUser(Connection conn) throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;
		int rVal = 0;

		try
		{
			st = conn.prepareStatement(MAX_USER_STR);
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
		boolean retry = true;
		
		while(retry)
		{
			try
			{
				this.createUsers();
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
						TimeUnit.MINUTES.sleep(5L);
						retry = true;
					}
					else
						retry = false;
				}
				catch(Exception ec) { }
			}
		}
	}
}