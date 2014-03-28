import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import db.DBConnector;
import db.InsertRepos;
import db.UpdateRepos;
//import db.InsertUsers;
//import db.UpdateUsers;

public class Main
{
	public static void main(String[] args)
	{
		/*Thread tui;/**/
		/*Thread tuu;/**/
		Thread tri;/**/
		Thread tru;/**/

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date;

		try
		{
			DBConnector dbc = new DBConnector();
//			tui = new Thread(new InsertUsers(dbc));
//			tui.start();
//			
//			tuu = new Thread(new UpdateUsers(dbc));
//			tuu.start();

			tri = new Thread(new InsertRepos(dbc));
			tri.start();
			
			tru = new Thread(new UpdateRepos(dbc, true, true));
			tru.start();
			
			while (
//					tui.isAlive() ||
//					tuu.isAlive() ||
					tri.isAlive() ||
					tru.isAlive()
					)
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