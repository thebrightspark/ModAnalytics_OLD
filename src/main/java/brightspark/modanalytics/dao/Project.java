package brightspark.modanalytics.dao;

import brightspark.modanalytics.db.DbConnection;
import brightspark.modanalytics.db.DbStorable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class Project extends DbStorable
{
	private final int id;
	private final String name;

	public Project(ResultSet resultSet) throws SQLException
	{
		this(resultSet.getInt("id"), resultSet.getString("name"));
	}

	public Project(int id, String name)
	{
		this.id = id;
		this.name = name;
	}

	public int getId()
	{
		return id;
	}

	public String getName()
	{
		return name;
	}

	@Override
	public String getTableName()
	{
		return DbConnection.TABLE_PROJECTS;
	}

	@Override
	public void getData(Map<String, Object> data)
	{
		data.put("id", id);
		data.put("name", name);
	}

	@Override
	public int hashCode()
	{
		return id;
	}

	@Override
	public boolean equals(Object obj)
	{
		if(obj == null) return false;
		if(obj == this) return true;
		if(obj.getClass() != getClass()) return false;
		return id == ((Project) obj).id;
	}
}
