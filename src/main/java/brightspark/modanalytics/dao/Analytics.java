package brightspark.modanalytics.dao;

import brightspark.modanalytics.db.DbConnection;
import brightspark.modanalytics.db.DbStorable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class Analytics extends DbStorable
{
	private final int id;
	private final int projectId;
	private final AnalyticDate date;
	private final float points;
	private final int historicalDownload;
	private final int dailyDownload;
	private final int dailyUniqueDownload;
	private final int dailyTwitchAppDownload;
	private final int dailyCurseForgeDownload;

	public Analytics(ResultSet resultSet) throws SQLException
	{
		id = resultSet.getInt("id");
		projectId = resultSet.getInt("project_id");
		date = new AnalyticDate(resultSet);
		points = resultSet.getFloat("points");
		historicalDownload = resultSet.getInt("historical_download");
		dailyDownload = resultSet.getInt("daily_download");
		dailyUniqueDownload = resultSet.getInt("daily_unique_download");
		dailyTwitchAppDownload = resultSet.getInt("daily_twitch_app_download");
		dailyCurseForgeDownload = resultSet.getInt("daily_curseforge_download");
	}

	public int getId()
	{
		return id;
	}

	public int getProjectId()
	{
		return projectId;
	}

	public AnalyticDate getDate()
	{
		return date;
	}

	public float getPoints()
	{
		return points;
	}

	public int getHistoricalDownload()
	{
		return historicalDownload;
	}

	public int getDailyDownload()
	{
		return dailyDownload;
	}

	public int getDailyUniqueDownload()
	{
		return dailyUniqueDownload;
	}

	public int getDailyTwitchAppDownload()
	{
		return dailyTwitchAppDownload;
	}

	public int getDailyCurseForgeDownload()
	{
		return dailyCurseForgeDownload;
	}

	@Override
	public String getTableName()
	{
		return DbConnection.TABLE_ANALYTICS;
	}

	@Override
	public void getData(Map<String, Object> data)
	{
		data.put("id", id);
		data.put("projectId", projectId);
		data.put("date", date);
		data.put("points", points);
		data.put("historicalDownload", historicalDownload);
		data.put("dailyDownload", dailyDownload);
		data.put("dailyUniqueDownload", dailyUniqueDownload);
		data.put("dailyTwitchAppDownload", dailyTwitchAppDownload);
		data.put("dailyCurseForgeDownload", dailyCurseForgeDownload);
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
		return id == ((Analytics) obj).id;
	}
}
