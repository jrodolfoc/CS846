package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;

import org.eclipse.egit.github.core.client.GitHubClient;

public class DBConnector
{
	private static String DBHOST = "jdbc:mysql://localhost:3306/github";
	private static String DBUSER = "cs846";
	private static String DBPASS = "jose";
	
	private static String CWL_VARS = "SELECT var_int_value, var_str_value FROM cwl_vars WHERE description = ? LIMIT 1";
	private static String UPD_CWL_VARS = "UPDATE cwl_vars SET var_int_value = ?, var_str_value = ? WHERE description = ? LIMIT 1";
	private int AuthKeyNo = 0;

	public Connection newConn() throws SQLException
	{
		Connection _con = null;

		try
	    {
			_con = DriverManager.getConnection(DBHOST, DBUSER, DBPASS);
			_con.setAutoCommit(true);
	    }
        catch (SQLException ex)
	    {
	        System.out.println(ex.getMessage());
	        _con = null;
	        throw ex;
	    }
		
		return _con;
	}

	public long CrawlerIntVar(Connection conn, String desc) throws SQLException
	{
		return (long) CrawlerVar(conn, desc, true);
	}
	
	public String CrawlerStrVar(Connection conn, String desc) throws SQLException
	{
		return (String) CrawlerVar(conn, desc, false);
	}

	private Object CrawlerVar(Connection conn, String desc, boolean isInt) throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;
		Object rVal = 0;

		try
		{
			st = conn.prepareStatement(CWL_VARS);
			st.setString(1, desc);
			rs = st.executeQuery();

			while (rs.next())
			{
				if(isInt)
					rVal = rs.getLong("var_int_value");
				else
					rVal = rs.getString("var_str_value");
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

	public void UpdateCrawlerIntVar(Connection conn, String desc, long val) throws SQLException
	{
		this.UpdateCrawlerVar(conn, desc, val);
	}

	public void UpdateCrawlerStrVar(Connection conn, String desc, String val) throws SQLException
	{
		this.UpdateCrawlerVar(conn, desc, val);
	}

	private void UpdateCrawlerVar(Connection conn, String desc, Object val) throws SQLException
	{
		PreparedStatement st = null;
		
		try
		{
			st = conn.prepareStatement(UPD_CWL_VARS);
			
			if(val instanceof Long)
			{
				st.setLong(1, (long) val);
				st.setNull(2, java.sql.Types.CHAR);
			}
			else
			{
				st.setString(2, (String) val);
				st.setNull(1, java.sql.Types.INTEGER);
			}

			st.setString(3, desc);
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

	public synchronized GitHubClient getGHClient(Connection conn) throws InterruptedException, SQLException
	{
		GitHubClient client = new GitHubClient();
		int i = this.AuthKeyNo++ % 10;
		String token = "";

		try
		{
			switch(i)
			{
			case 0:
				token = CrawlerStrVar(conn, "joseauth");
				break;
			case 1:
				token = CrawlerStrVar(conn, "mdhrauth");
				break;
			case 2:
				token = CrawlerStrVar(conn, "jsphauth");
				break;
			case 3:
				token = CrawlerStrVar(conn, "desiauth");
				break;
			case 4:
				token = CrawlerStrVar(conn, "ubu1auth");
				break;
			case 5:
				token = CrawlerStrVar(conn, "ubu2auth");
				break;
			case 6:
				token = CrawlerStrVar(conn, "ubu3auth");
				break;
			case 7:
				token = CrawlerStrVar(conn, "ubu7auth");
				break;
			case 8:
				token = CrawlerStrVar(conn, "ubu8auth");
				break;
			case 9:
				TimeUnit.MINUTES.sleep(10L);
				break;
			}
		}
		catch(Exception ec) { }

		client.setOAuth2Token(token);
		return client;
	}
}