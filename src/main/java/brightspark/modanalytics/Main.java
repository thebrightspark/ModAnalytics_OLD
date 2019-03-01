package brightspark.modanalytics;

import brightspark.modanalytics.dao.Analytics;
import brightspark.modanalytics.dao.Project;
import brightspark.modanalytics.db.DbConnection;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
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

	private static final Object lock = new Object();
	private static Logger log = LogManager.getLogger(Main.class);
	protected static DbConnection db;
	private static ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	//private static List<Integer> projectIds = new LinkedList<>();

	public static void main(String... args)
	{
		init();

		//Quit when we get "stop" from the console
		log.info("Enter 'stop' to shutdown\n");
		Console console = System.console();
		if(console != null)
		{
			while(true)
			{
				String line = console.readLine();
				if("stop".equalsIgnoreCase(line))
				{
					shutdown();
					break;
				}
				else
					tryExecuteQuery(line);
			}
		}
		else
		{
			//Fallback for dev environment
			try(Scanner scanner = new Scanner(System.in))
			{
				while(true)
				{
					String line = scanner.nextLine();
					if("stop".equalsIgnoreCase(line))
					{
						shutdown();
						break;
					}
					else
						tryExecuteQuery(line);
				}
			}
		}
		System.exit(0);
	}

	protected static void tryExecuteQuery(String query)
	{
		if(StringUtils.isEmpty(query.trim()))
			return;
		try
		{
			System.out.println(db.executeSingleResult(query, Main::resultsToString));
		}
		catch(Exception e)
		{
			log.error("Error trying to use console input as query", e);
		}
	}

	protected static String resultsToString(ResultSet results) throws SQLException
	{
		ResultSetMetaData metaData = results.getMetaData();
		int numColumns = metaData.getColumnCount();

		List<List<String>> resultTable = new ArrayList<>();
		List<String> columnNames = new ArrayList<>();
		for(int c = 1; c <= numColumns; c++)
			columnNames.add(metaData.getColumnName(c));
		resultTable.add(columnNames);

		//Collect all results into lists
		do
		{
			List<String> resultRow = new ArrayList<>();
			for(int i = 1; i <= numColumns; i++)
				resultRow.add(results.getString(i));
			resultTable.add(resultRow);
		}
		while(results.next());

		//Find the largest value width for each column
		List<Integer> largestWidths = new ArrayList<>(Collections.nCopies(numColumns, 0));
		for(List<String> rowData : resultTable)
		{
			for(int column = 0; column < numColumns; column++)
			{
				String value = rowData.get(column);
				int valueLength = value.length();
				if(valueLength > largestWidths.get(column))
					largestWidths.set(column, valueLength);
			}
		}

		StringBuilder sb = new StringBuilder(" ");
		//Column names
		for(int i = 0; i < numColumns; i++)
		{
			sb.append(pad(columnNames.get(i), largestWidths.get(i)));
			if(i < numColumns - 1)
				sb.append(" | ");
		}

		sb.append(" \n-");

		//Separator
		for(int i = 0; i < numColumns; i++)
		{
			sb.append(StringUtils.repeat("-", largestWidths.get(i)));
			if(i < numColumns - 1)
				sb.append("-+-");
		}

		sb.append("-");

		//Result rows
		for(int row = 1; row < resultTable.size(); row++)
		{
			List<String> rowList = resultTable.get(row);
			sb.append("\n ");
			for(int i = 0; i < numColumns; i++)
			{
				sb.append(pad(rowList.get(i), largestWidths.get(i)));
				if(i < numColumns - 1)
					sb.append(" | ");
			}
			sb.append(" ");
		}

		return sb.toString();
	}

	private static String pad(String value, int maxLength)
	{
		int valueLength = value.length();
		return StringUtils.repeat(" ", maxLength - valueLength) + value;
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private static void init()
	{
		//handleProperties();

		//Make sure CSV directories is created
		CSV_INPUT_DIR.mkdirs();
		CSV_PROCESSED_DIR.mkdir();
		CSV_FAILED_DIR.mkdir();

		//Setup CSV input checker
		scheduledExecutor.scheduleAtFixedRate(Main::processCSVs, 5, 30, TimeUnit.SECONDS);

		//Setup SQLite DB
		try
		{
			Class.forName("org.sqlite.JDBC");
		}
		catch(ClassNotFoundException e)
		{
			log.error("Couldn't initialise JDBC", e);
			System.exit(0);
		}
		db = new DbConnection(null);
	}

	private static void shutdown()
	{
		log.info("Shutting down...");
		synchronized(lock)
		{
			scheduledExecutor.shutdown();
		}
	}

	private static void processCSVs()
	{
		synchronized(lock)
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
				boolean result = false;
				try
				{
					result = processCSV(file);
				}
				catch(Exception e)
				{
					log.error(String.format("Failed to process CSV %s", file.getName()), e);
				}

				if(result)
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
