import db.DBConnector;
import db.Users;

public class Main
{
	public static void main(String[] args)
	{
		try
		{
			DBConnector dbc = new DBConnector();
			Users us = new Users(dbc);
			us.InsertUsers();
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
}