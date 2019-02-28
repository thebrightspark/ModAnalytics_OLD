package brightspark.modanalytics;

import brightspark.modanalytics.dao.Analytics;
import brightspark.modanalytics.dao.Project;
import brightspark.modanalytics.db.DbConnection;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main
{
	//private static final File CONFIG_FILE = new File("config.properties");
	private static final File CSV_DIR = new File("csv");
	private static final File CSV_INPUT_DIR = new File(CSV_DIR, "input");
	private static final File CSV_PROCESSED_DIR = new File(CSV_DIR, "processed");
	private static final File CSV_FAILED_DIR = new File(CSV_DIR, "failed");
	private static final CSVParser CSV_PARSER = new CSVParser();

	private static Logger log = LogManager.getLogger(Main.class);
	protected static DbConnection db;
	private static ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	//private static List<Integer> projectIds = new LinkedList<>();

	public static void main(String... args)
	{
		//TODO: Have a way to input the CSV files
		// Maybe check a certain directory every 10s and import (then delete/move) the CSVs into the DB
		init();
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private static void init()
	{
		//handleProperties();

		//Make sure CSV directories is created
		CSV_INPUT_DIR.mkdirs();
		CSV_PROCESSED_DIR.mkdir();

		//Setup CSV input checker
		scheduledExecutor.scheduleAtFixedRate(Main::processCSVs, 1, 10, TimeUnit.MINUTES);

		//Setup SQLite DB
		try
		{
			db = new DbConnection(null);
		}
		catch(SQLException e)
		{
			log.error("Couldn't connect to the DB", e);
		}
		catch(ClassNotFoundException e)
		{
			log.error("Couldn't initialise JDBC", e);
		}
	}

	private static void processCSVs()
	{
		File[] files = CSV_INPUT_DIR.listFiles();
		if(files == null)
		{
			log.error("Problem getting files from {}", CSV_INPUT_DIR.getAbsolutePath());
			return;
		}
		if(files.length <= 0)
		{
			log.debug("No CSVs to process in {}", CSV_INPUT_DIR.getPath());
			return;
		}

		log.info("Found {} CSVs to process", files.length);
		for(File file : files)
		{
			if(processCSV(file))
			{
				if(!file.renameTo(new File(CSV_PROCESSED_DIR, file.getName())))
					log.warn("Failed to move CSV {} to the processed directory!", file.getName());
			}
			else
			{
				log.warn("Failed to process {} - will move it to failed directory");
				if(!file.renameTo(new File(CSV_FAILED_DIR, file.getName())))
					log.warn("Failed to move CSV {} to the failed directory!", file.getName());
			}
		}

		log.info("Finished processing CSVs");
	}

	protected static boolean processCSV(File file)
	{
		log.info("Processing CSV {}", file.getPath());
		List<String[]> rows = null;
		try(CSVReader reader = new CSVReaderBuilder(new FileReader(file))
			.withSkipLines(1).withCSVParser(CSV_PARSER).build())
		{
			rows = reader.readAll();
		}
		catch(FileNotFoundException e)
		{
			log.error(String.format("CSV file %s couldn't be found", file.getPath()), e);
		}
		catch(IOException e)
		{
			log.error(String.format("Failed to read CSV file %s", file.getPath()), e);
		}
		if(rows == null)
			return false;

		log.info("Read {} rows from CSV", rows.size());

		//Process each row of the CSV
		boolean ensuredProject = false;
		for(String[] row : rows)
		{
			log.trace("Processing row: {}", Arrays.toString(row));
			Analytics analytics = new Analytics(row);
			//Check that the project exists in the DB - if not, add it
			if(!ensuredProject)
			{
				ensuredProject = true;
				Project project;
				try
				{
					project = db.executeSingleResult("select * from " + DbConnection.TABLE_PROJECTS + " where id = " + analytics.getProjectId(), Project::new);
				}
				catch(RuntimeException e)
				{
					log.error("Failed to get project from DB with id " + analytics.getProjectId(), e);
					return false;
				}

				if(project == null)
				{
					project = new Project(analytics.getProjectId(), row[2]);
					log.info("{} doesn't exist in DB - adding now", project);
					db.insert(project);
				}
			}

			//Update the DB with each of the analytics
			db.insert(analytics);
		}

		log.info("CSV {} processed - added/updated {} analytics in DB", file.getPath(), rows.size());
		return true;
	}



	/*private static void handleProperties()
	{
		Properties properties = new Properties();
		if(!CONFIG_FILE.exists())
		{
			//If not config file, then generate default and exit
			log.error("No config file found at {}. Generating default but user must enter project IDs into file.", CONFIG_FILE.getAbsolutePath());
			properties.setProperty("projectIds", "");
			try(OutputStream output = new FileOutputStream(CONFIG_FILE))
			{
				properties.store(output, null);
			}
			catch(IOException e)
			{
				log.error("Failed to create default config file at " + CONFIG_FILE.getAbsolutePath(), e);
			}
			return;
		}

		//Load properties
		try(InputStream input = new FileInputStream(CONFIG_FILE))
		{
			properties.load(input);
		}
		catch(IOException e)
		{
			log.error("Failed to load config file at " + CONFIG_FILE.getAbsolutePath(), e);
			System.exit(0);
		}

		//Read properties
		String projectIdsString = properties.getProperty("projectIds").replaceAll(" ", "");
		if(projectIdsString.isEmpty())
		{
			log.error("No project IDs defined in config file {}", CONFIG_FILE.getAbsolutePath());
		}
		for(String idString : projectIdsString.split(","))
		{
			try
			{
				projectIds.add(Integer.parseInt(idString));
			}
			catch(NumberFormatException e)
			{
				log.error("Couldn't parse project ID '{}' as an integer", idString);
			}
		}
		if(projectIds.isEmpty())
		{
			log.error("No project IDs successfully read from config file " + CONFIG_FILE.getAbsolutePath());
			System.exit(0);
		}

		log.info("Read project IDs: {}", projectIds);
	}*/

	/*private static void downloadLatestAnalytics() throws IOException
	{
		//TODO: Get CSV data
		String content;
		CloseableHttpClient client = HttpClients.createDefault();
		try
		{
			content = client.execute(new HttpGet("https://minecraft.curseforge.com/dashboard/project/237240/exportcsv"), response ->
			{
				int status = response.getStatusLine().getStatusCode();
				if(status >= 200 && status < 300)
				{
					HttpEntity entity = response.getEntity();
					return entity == null ? null : EntityUtils.toString(entity);
				}
				return null;
			});
		}
		finally
		{
			client.close();
		}
		System.out.println(content);
	}*/
}
