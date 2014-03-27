import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import db.DBConnector;
//import db.InsertUsers;
import db.UpdateUsers;

public class Main
{
	public static void main(String[] args)
	{
//		Thread ti;
		Thread tu;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date;

		try
		{
			DBConnector dbc = new DBConnector();
//			ti = new Thread(new InsertUsers(dbc));
//			ti.start();
			
			tu = new Thread(new UpdateUsers(dbc));
			tu.start();
			
			while (/*ti.isAlive() ||*/ tu.isAlive())
			{
				date = new Date();
				System.out.println("Inside loop " + " [" + dateFormat.format(date) + "]");
				TimeUnit.SECONDS.sleep(500);
			}
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
}