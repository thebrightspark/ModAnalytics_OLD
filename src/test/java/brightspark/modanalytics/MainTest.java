package brightspark.modanalytics;

import brightspark.modanalytics.dao.Project;
import brightspark.modanalytics.db.DbConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

class MainTest
{
	@Mock
	private DbConnection dbConnection;

	private File exampleAnalyticsFile;

	@BeforeEach
	void setup()
	{
		MockitoAnnotations.initMocks(this);
		//System.out.println(Thread.currentThread().getContextClassLoader().getResource("."));
		exampleAnalyticsFile = new File(Thread.currentThread().getContextClassLoader().getResource("ExampleAnalytics.csv").getPath());
		Main.db = dbConnection;
	}

	@Test
	void testProcessCSV()
	{
		final int[] added = {0};
		doAnswer(invocationOnMock -> added[0]++).when(dbConnection).insert(any());
		doReturn(new Project(238858,"Glowing Glass")).when(dbConnection).executeSingleResult(anyString(), any());

		assertTrue(Main.processCSV(exampleAnalyticsFile));
		assertEquals(91, added[0]);
	}
}