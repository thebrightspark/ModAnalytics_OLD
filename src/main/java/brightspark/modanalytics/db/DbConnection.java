package brightspark.modanalytics.db;

import brightspark.modanalytics.ResultParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class DbConnection
{
	private static final Logger log = LogManager.getLogger(DbConnection.class);
	public static final String TABLE_PROJECTS = "projects";
	public static final String TABLE_ANALYTICS = "analytics";

	private static final String QUERY_CREATE = "create table if not exists %s (%s)";
	public static final String QUERY_INSERT = "replace into %s (%s) values (%s)";

	private String location;
	private Connection connection;

	public DbConnection(File file)
	{
		location = file == null ? ":memory:" : file.getAbsolutePath();

		//Create tables if they don't already exist
		execute(String.format(QUERY_CREATE, TABLE_PROJECTS,
			"id integer primary key, " +
			"name text not null"));
		execute(String.format(QUERY_CREATE, TABLE_ANALYTICS,
			"id integer primary key autoincrement, " +
			"project_id integer not null, " +
			"date text not null, " +
			"points real not null, " +
			"historical_download integer not null, " +
			"daily_download integer not null, " +
			"daily_unique_download integer not null, " +
			"daily_twitch_app_download integer not null, " +
			"daily_curseforge_download integer not null"));

		//Log the tables and their columns
		if(log.isDebugEnabled())
		{
			log.debug("Tables:");
			List<String> tables = execute("select name from sqlite_master where type = 'table'", results -> results.getString("name"));
			tables.forEach(table ->
			{
				List<String> rows = execute("pragma table_info('" + table + "')", results ->
					String.format("%s (type: %s, pk: %s)", results.getString("name"), results.getString("type"), results.getInt("pk")));
				log.debug("{} -> {}", table, rows);
			});
		}
	}

	/**
	 * Gets a connection to the DB
	 */
	private Connection getConnection()
	{
		try
		{
			return connection != null && !connection.isClosed() ?
					connection :
					(connection = DriverManager.getConnection("jdbc:sqlite:" + location));
		}
		catch(SQLException e)
		{
			log.error("Couldn't open connection to DB", e);
			System.exit(0);
		}
		return null;
	}

	/**
	 * Executes a query on the DB and returns no result
	 * @param query Query to execute
	 */
	public void execute(String query)
	{
		log.debug("Executing query: {}", query);
		try(Statement statement = getConnection().createStatement())
		{
			statement.execute(query);
			log.trace("Executed query");
		}
		catch(SQLException e)
		{
			log.error(String.format("Couldn't execute query '%s'", query), e);
		}
	}

	/**
	 * Executes a query on the DB and returns the results as formatted by the function
	 * @param query Query to execute
	 * @param resultParser Function to format the results
	 * @param <T> The type the function will return
	 * @return The list of objects as a result of the function
	 */
	public <T> List<T> execute(String query, ResultParser<T> resultParser)
	{
		log.debug("Executing query: {}", query);
		try(Statement statement = getConnection().createStatement();
		    ResultSet results = statement.executeQuery(query))
		{
			List<T> resultList = new LinkedList<>();
			while(results.next())
				resultList.add(resultParser.apply(results));
			log.trace("Executed query");
			return resultList;
		}
		catch(SQLException e)
		{
			log.error(String.format("Error parsing results from query '%s'", query), e);
		}
		return null;
	}

	/**
	 * Executes a query on the DB that expects a single result and returns the result as formatted by the function
	 * @param query Query to execute
	 * @param resultParser Function to format the result
	 * @param <T> The type the function will return
	 * @return The object as a result of the function
	 */
	public <T> T executeSingleResult(String query, ResultParser<T> resultParser)
	{
		List<T> resultList = execute(query, resultParser);
		Integer size = null;
		if(resultList == null || (size = resultList.size()) > 1)
			throw new RuntimeException(String.format("Expected 1 result, but got %s! Query -> %s", size, query));
		return size == 0 ? null : resultList.get(0);
	}

	/**
	 * Inserts or updates the object in the DB
	 */
	public void insert(DbStorable storable)
	{
		try(PreparedStatement statement = storable.createStatement(getConnection()))
		{
			log.debug("Executing insert of {}", storable);
			statement.executeUpdate();
			log.trace("Executed query");
		}
		catch(SQLException e)
		{
			log.error("Couldn't create query", e);
		}
	}

	private void setAllValuesToStatement(PreparedStatement statement, List<Object> values, int index) throws SQLException
	{
		for(Object value : values)
		{
			if(value instanceof String)
				statement.setString(++index, (String) value);
			else if(value instanceof Integer)
				statement.setInt(++index, (Integer) value);
			else if(value instanceof Float)
				statement.setFloat(++index, (Float) value);
			else if(value instanceof DbStorable)
				setAllValuesToStatement(statement, ((DbStorable) value).getData().valueList(), index);
			else
				throw new RuntimeException("Unhandled value type -> " + value);
		}
	}
}
