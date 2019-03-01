package brightspark.modanalytics.db;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class DbStorable
{
	protected String insertQuery = String.format(DbConnection.QUERY_INSERT, getTableName(), getColumns(), getValuePlaceholders());

	/**
	 * Gets the table name for this type
	 */
	protected abstract String getTableName();

	/**
	 * Gets the table columns for this type used for the insert query
	 */
	protected abstract String getColumns();

	/**
	 * Gets the a comma separated String of '?' for the insert query
	 */
	private String getValuePlaceholders()
	{
		return StringUtils.repeat("?", ",", StringUtils.countMatches(getColumns(), ',') + 1);
	}

	/**
	 * Gets all of the data in this object and returns it in a {@link ListOrderedMap}
	 */
	public final ListOrderedMap<String, Object> getData()
	{
		ListOrderedMap<String, Object> map = new ListOrderedMap<>();
		getData(map);
		return map;
	}

	/**
	 * Puts all of the data in this object into the map
	 */
	protected abstract void getData(Map<String, Object> data);

	/**
	 * Gets all of the data in this object and any nested {@link DbStorable} objects and returns it in a {@link ListOrderedMap}
	 */
	public ListOrderedMap<String, Object> getAllData()
	{
		return getAllData(getData());
	}

	/**
	 * Puts all of the data in this object and any nested {@link DbStorable} objects into the map
	 */
	private ListOrderedMap<String, Object> getAllData(ListOrderedMap<String, Object> data)
	{
		ListOrderedMap<String, Object> allData = new ListOrderedMap<>();
		for(Map.Entry<String, Object> entry : data.entrySet())
		{
			Object value = entry.getValue();
			if(value instanceof DbStorable)
				allData.putAll(getAllData(((DbStorable) value).getData()));
			else
				allData.put(entry.getKey(), entry.getValue());
		}
		return allData;
	}

	public abstract PreparedStatement createStatement(Connection connection) throws SQLException;

	@Override
	public String toString()
	{
		ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
		Map<String, Object> data = new HashMap<>();
		getData(data);
		data.forEach(builder::append);
		return builder.toString();
	}
}
