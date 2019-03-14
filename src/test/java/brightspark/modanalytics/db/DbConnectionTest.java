package brightspark.modanalytics.db;

import brightspark.modanalytics.dao.Analytics;
import brightspark.modanalytics.dao.Project;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DbConnectionTest
{
	private DbConnection db;

	@BeforeEach
	void setUp()
	{
		db = new DbConnection(null);
	}

	@Test
	void testInsertProject()
	{
		Project project = new Project(238858, "Glowing Glass");
		db.insert(project);

		Project project1 = db.executeSingleResult("select * from projects where id = 238858", Project::new);

		assertEquals(project.getId(), project1.getId());
		assertEquals(project.getName(), project1.getName());
	}

	private void assertAnalyticsEqual(Analytics a1, Analytics a2)
	{
		assertEquals(a1.getId(), a2.getId());
		assertEquals(a1.getProjectId(), a2.getProjectId());
		assertEquals(a1.getDate(), a2.getDate());
		assertEquals(a1.getPoints(), a2.getPoints());
		assertEquals(a1.getHistoricalDownload(), a1.getHistoricalDownload());
		assertEquals(a1.getDailyDownload(), a2.getDailyDownload());
		assertEquals(a1.getDailyUniqueDownload(), a2.getDailyUniqueDownload());
		assertEquals(a1.getDailyTwitchAppDownload(), a2.getDailyTwitchAppDownload());
		assertEquals(a1.getDailyCurseForgeDownload(), a2.getDailyCurseForgeDownload());
	}

	@Test
	void testInsertAnalytic()
	{
		Analytics analytics = new Analytics("238858_2018-12-01", 238858, "2018-12-01", 0, 10130, 10, 10, 7, 3);
		db.insert(analytics);

		Analytics analytics1 = db.executeSingleResult("select * from analytics where id = '238858_2018-12-01'", Analytics::new);
		assertAnalyticsEqual(analytics, analytics1);
	}

	@Test
	void testUpdatingRows()
	{
		db.insert(new Project(238858, "Glowing Glass"));
		Analytics analytics1 = new Analytics("238858_2018-12-01", 238858, "2018-12-01", 0, 10130, 10, 10, 7, 3);
		db.insert(analytics1);
		Analytics analytics2 = new Analytics("238858_2018-12-01", 238858, "2018-12-01", 0, 0, 0, 0, 0, 0);
		db.insert(analytics2);

		int count = db.executeSingleResult("select count(*) from analytics;", results -> results.getInt(1));
		assertEquals(1, count);
		Analytics analytics3 = db.executeSingleResult("select * from analytics where id = '238858_2018-12-01'", Analytics::new);
		assertAnalyticsEqual(analytics2, analytics3);
	}
}