package brightspark.modanalytics;

import brightspark.modanalytics.dao.Project;
import brightspark.modanalytics.db.DbConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MainTest
{
	private Main main;
	private File exampleAnalyticsFile;

	@BeforeEach
	void setup()
	{
		//MockitoAnnotations.initMocks(this);
		//System.out.println(Thread.currentThread().getContextClassLoader().getResource("."));
		main = new Main();
		exampleAnalyticsFile = new File(Thread.currentThread().getContextClassLoader().getResource("ExampleAnalytics.csv").getPath());
	}

	@Test
	void testProcessCSV()
	{
		Main.db = mock(DbConnection.class);

		final int[] added = {0};
		doAnswer(invocationOnMock -> added[0]++).when(Main.db).insert(any());
		doReturn(new Project(238858,"Glowing Glass")).when(Main.db).executeSingleResult(anyString(), any());

		assertTrue(main.processCSV(exampleAnalyticsFile));
		assertEquals(91, added[0]);
	}

	@Test
	void testResultsToString()
	{
		Main.db = new DbConnection(null);

		Main.db.execute("insert into projects (id, name) values (1, 'test1')");
		Main.db.execute("insert into projects (id, name) values (2, 'test2')");

		String text = Main.db.executeSingleResult("select * from projects", main::resultsToString);
		System.out.println(text);
		assertEquals(
			" id |  name \n" +
			"----+-------\n" +
			"  1 | test1 \n" +
			"  2 | test2 ", text);
	}
}