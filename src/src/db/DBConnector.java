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
	private Connection _con;
	private int AuthKeyNo = 0;

	private void connect() throws SQLException
	{
		this._con = null;

		try
	    {
			this._con = DriverManager.getConnection(DBHOST, DBUSER, DBPASS);
			this._con.setAutoCommit(true);
	    }
        catch (SQLException ex)
	    {
	        System.out.println(ex.getMessage());
	        this._con = null;
	        throw ex;
	    }
	}

	public Connection getConn() throws SQLException
	{
		if(this._con == null)
		{
			try
		    {
				this.connect();
		    }
	        catch (SQLException ex)
		    {
	        	throw ex;
		    }
		}
		
		return this._con;
	}

	public int CrawlerIntVar(String desc) throws SQLException
	{
		return (int) CrawlerVar(desc, true);
	}
	
	public String CrawlerStrVar(String desc) throws SQLException
	{
		return (String) CrawlerVar(desc, false);
	}

	private Object CrawlerVar(String desc, boolean isInt) throws SQLException
	{
		PreparedStatement st = null;
		ResultSet rs = null;
		Object rVal = 0;

		try
		{
			st = this.getConn().prepareStatement(CWL_VARS);
			st.setString(1, desc);
			rs = st.executeQuery();

			while (rs.next())
			{
				if(isInt)
					rVal = rs.getInt("var_int_value");
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

	public void UpdateCrawlerIntVar(String desc, int val) throws SQLException
	{
		this.UpdateCrawlerVar(desc, val);
	}

	public void UpdateCrawlerStrVar(String desc, String val) throws SQLException
	{
		this.UpdateCrawlerVar(desc, val);
	}

	private void UpdateCrawlerVar(String desc, Object val) throws SQLException
	{
		PreparedStatement st = null;
		
		try
		{
			st = this.getConn().prepareStatement(UPD_CWL_VARS);
			
			if(val instanceof Integer)
			{
				st.setInt(1, (int) val);
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

	public GitHubClient getGHClient() throws InterruptedException, SQLException
	{
		GitHubClient client = new GitHubClient();
		int i = this.AuthKeyNo++ % 3;
		String token = "";

		switch(i)
		{
		case 0:
			token = CrawlerStrVar("joseauth");
			break;
		case 1:
			TimeUnit.MINUTES.sleep(10L);
//			token = CrawlerStrVar("mdhrauth");
			break;
		case 2:
			token = CrawlerStrVar("jsphauth");
			break;
		default:
			TimeUnit.MINUTES.sleep(10L);
			break;
		}

		client.setOAuth2Token(token);
		return client;
	}
}