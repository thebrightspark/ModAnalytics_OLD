package brightspark.modanalytics;

import brightspark.modanalytics.dao.Analytics;
import brightspark.modanalytics.dao.Project;
import brightspark.modanalytics.db.DbConnection;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
	private static final File DEFAULT_DB_FILE = null; //TODO: Change this when done testing
	private static final File DEFAULT_CSV_DIR = new File("csv");
	private File csvInputDir = new File(DEFAULT_CSV_DIR, "input");
	private File csvProcessedDir = new File(DEFAULT_CSV_DIR, "processed");
	private File csvFailedDir = new File(DEFAULT_CSV_DIR, "failed");

	private static final CSVParser CSV_PARSER = new CSVParser();
	private static final Object lock = new Object();
	private static Logger log = LogManager.getLogger(Main.class);
	static DbConnection db;
	private static ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

	@Parameter(names = "-help", description = "Display this help", help = true)
	private boolean help;

	@Parameter(names = "-file", description = "Specific file to process. Can't be used with -dir", validateWith = FileParamValidator.class)
	private String filePath;

	@Parameter(names = "-dir", description = "Directory path. Can't be used with -file", validateWith = DirParamValidator.class)
	private String dirPath;

	@Parameter(names = "-db", description = "Database file location", validateWith = FileParamValidator.class)
	private String dbPath;

	public static void main(String... args)
	{
		Main main = new Main();

		JCommander jCommander = JCommander.newBuilder()
			.addObject(main)
			.args(args)
			.build();
		jCommander.setProgramName("ModAnalytics");

		boolean showUsage = main.help;

		if(main.filePath != null && main.dirPath != null)
		{
			log.warn("Can't use both '-file' and '-dir'!");
			showUsage = true;
		}
		if(showUsage)
		{
			jCommander.usage();
			return;
		}

		main.run();
		System.exit(0);
	}

	private void run()
	{
		init();

		if(filePath != null)
		{
			//Just process the single file then quit
			handleCSV(new File(filePath));
		}
		else
		{
			//Quit when we get "stop" from the console
			log.info("Enter 'stop' to shutdown\n");
			Console console = System.console();
			if(console != null)
			{
				String line;
				while(!"stop".equalsIgnoreCase(line = console.readLine()))
					tryExecuteQuery(line);
			}
			else
			{
				//Fallback for dev environment
				try(Scanner scanner = new Scanner(System.in))
				{
					String line;
					while(!"stop".equalsIgnoreCase(line = scanner.nextLine()))
						tryExecuteQuery(line);
				}
			}
		}
		shutdown();
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private void init()
	{
		File csvDir = dirPath == null ? DEFAULT_CSV_DIR : new File(dirPath);
		csvInputDir = new File(csvDir, "input");
		csvProcessedDir = new File(csvDir, "processed");
		csvFailedDir = new File(csvDir, "failed");

		//Make sure CSV directories is created
		csvInputDir.mkdirs();
		csvProcessedDir.mkdir();
		csvFailedDir.mkdir();

		//Setup CSV input checker
		if(filePath == null)
			scheduledExecutor.scheduleAtFixedRate(this::processCSVs, 5, 30, TimeUnit.SECONDS);

		//Setup SQLite DB
		try
		{
			Class.forName("org.sqlite.JDBC");
		}
		catch(ClassNotFoundException e)
		{
			log.error("Couldn't initialise JDBC", e);
		}
		db = new DbConnection(dbPath == null ? DEFAULT_DB_FILE : new File(dbPath));
	}

	private void shutdown()
	{
		log.info("Shutting down...");
		synchronized(lock)
		{
			scheduledExecutor.shutdown();
		}
	}

	private void tryExecuteQuery(String query)
	{
		if(StringUtils.isEmpty(query.trim()))
			return;
		try
		{
			System.out.println(db.executeSingleResult(query, this::resultsToString));
		}
		catch(Exception e)
		{
			log.error("Error trying to use console input as query", e);
		}
	}

	String resultsToString(ResultSet results) throws SQLException
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

	private String pad(String value, int maxLength)
	{
		int valueLength = value.length();
		return StringUtils.repeat(" ", maxLength - valueLength) + value;
	}

	private void processCSVs()
	{
		synchronized(lock)
		{
			File[] files = csvInputDir.listFiles();
			if(files == null)
			{
				log.error("Problem getting files from {}", csvInputDir.getAbsolutePath());
				return;
			}
			if(files.length <= 0)
			{
				log.debug("No CSVs to process in {}", csvInputDir.getPath());
				return;
			}

			log.info("Found {} CSVs to process", files.length);
			for(File file : files)
				handleCSV(file);

			log.info("Finished processing CSVs");
		}
	}

	private void handleCSV(File file)
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
			if(!file.renameTo(new File(csvProcessedDir, file.getName())))
				log.warn("Failed to move CSV {} to the processed directory!", file.getName());
		}
		else
		{
			log.warn("Failed to process {} - will move it to failed directory");
			if(!file.renameTo(new File(csvFailedDir, file.getName())))
				log.warn("Failed to move CSV {} to the failed directory!", file.getName());
		}
	}

	boolean processCSV(File file)
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
}
