package brightspark.modanalytics.db;

import brightspark.modanalytics.ResultParser;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.lang3.StringUtils;
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
	private static final String QUERY_INSERT = "replace into %s (%s) values (%s)";

	private Connection connection;

	public DbConnection(File file) throws SQLException, ClassNotFoundException
	{
		Class.forName("org.sqlite.JDBC");
		String location = file == null ? ":memory:" : file.getAbsolutePath();
		connection = DriverManager.getConnection("jdbc:sqlite:" + location);
		log.info("Connection established to DB at {}", location);

		//Create tables if they don't already exist
		execute(String.format(QUERY_CREATE, TABLE_PROJECTS,
			"id integer primary key, " +
			"name text"));
		execute(String.format(QUERY_CREATE, TABLE_ANALYTICS,
			"id integer primary key, " +
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
				log.info("{} -> {}", table, rows);
			});
		}
	}

	/**
	 * Executes a query on the DB and returns the results in a {@link ResultSet} if any
	 * @param query Query to execute
	 * @return Results
	 */
	public ResultSet execute(String query)
	{
		try
		{
			Statement statement = connection.createStatement();
			log.debug("Executing query: {}", query);
			boolean hasResults = statement.execute(query);
			return hasResults ? statement.getResultSet() : null;
		}
		catch(SQLException e)
		{
			log.error(String.format("Couldn't execute query '%s'", query), e);
		}
		return null;
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
		ResultSet results = execute(query);
		if(results == null)
			return null;
		List<T> resultList = new LinkedList<>();
		try
		{
			while(results.next())
				resultList.add(resultParser.apply(results));
		}
		catch(SQLException e)
		{
			log.error(String.format("Error parsing results from query '%s'", query), e);
		}
		return resultList;
	}

	/**
	 * Inserts or updates the object in the DB
	 */
	public void insert(DbStorable storable)
	{
		ListOrderedMap<String, Object> allData = storable.getAllData();
		String keys = String.join(",", allData.keyList());
		String valuePlaceholders = StringUtils.repeat("?", ",", allData.size());
		String query = String.format(QUERY_INSERT, storable.getTableName(), keys, valuePlaceholders);

		try
		{
			PreparedStatement statement = connection.prepareStatement(query);
			setAllValuesToStatement(statement, allData.valueList(), 0);
			statement.executeUpdate();
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
