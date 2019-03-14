package brightspark.modanalytics.dao;

import brightspark.modanalytics.db.DbConnection;
import brightspark.modanalytics.db.DbStorable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class Analytics extends DbStorable
{
	private final String id;
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
		// ID = <projectId>_<analyticDate>
		id = resultSet.getString("id");
		projectId = resultSet.getInt("project_id");
		date = new AnalyticDate(resultSet);
		points = resultSet.getFloat("points");
		historicalDownload = resultSet.getInt("historical_download");
		dailyDownload = resultSet.getInt("daily_download");
		dailyUniqueDownload = resultSet.getInt("daily_unique_download");
		dailyTwitchAppDownload = resultSet.getInt("daily_twitch_app_download");
		dailyCurseForgeDownload = resultSet.getInt("daily_curseforge_download");
	}

	public Analytics(String[] csvRow) throws RuntimeException
	{
		if(csvRow.length != 9)
			throw new RuntimeException(String.format("Invalid CSV data! Should be 9 values but there are %s", csvRow.length));

		date = new AnalyticDate(csvRow[0]);
		projectId = Integer.parseInt(csvRow[1]);
		id = projectId + "_" + date.getDate();
		points = Float.parseFloat(csvRow[3]);
		historicalDownload = Integer.parseInt(csvRow[4]);
		dailyDownload = Integer.parseInt(csvRow[5]);
		dailyUniqueDownload = Integer.parseInt(csvRow[6]);
		dailyTwitchAppDownload = Integer.parseInt(csvRow[7]);
		dailyCurseForgeDownload = Integer.parseInt(csvRow[8]);
	}

	public Analytics(String id, int projectId, String date, float points, int historicalDownload, int dailyDownload, int dailyUniqueDownload, int dailyTwitchAppDownload, int dailyCurseForgeDownload)
	{
		this.id = id;
		this.projectId = projectId;
		this.date = new AnalyticDate(date);
		this.points = points;
		this.historicalDownload = historicalDownload;
		this.dailyDownload = dailyDownload;
		this.dailyUniqueDownload = dailyUniqueDownload;
		this.dailyTwitchAppDownload = dailyTwitchAppDownload;
		this.dailyCurseForgeDownload = dailyCurseForgeDownload;
	}

	public String getId()
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
	protected String getColumns()
	{
		return "id,project_id,date,points,historical_download,daily_download,daily_unique_download,daily_twitch_app_download,daily_curseforge_download";
	}

	@Override
	public void getData(Map<String, Object> data)
	{
		data.put("id", id);
		data.put("project_id", projectId);
		data.put("date", date.toString());
		data.put("points", points);
		data.put("historical_download", historicalDownload);
		data.put("daily_download", dailyDownload);
		data.put("daily_unique_download", dailyUniqueDownload);
		data.put("daily_twitch_app_download", dailyTwitchAppDownload);
		data.put("daily_curseforge_download", dailyCurseForgeDownload);
	}

	@Override
	public PreparedStatement createStatement(Connection connection) throws SQLException
	{
		PreparedStatement statement = connection.prepareStatement(insertQuery);
		statement.setString(1, id);
		statement.setInt(2, projectId);
		statement.setString(3, date.getDate());
		statement.setFloat(4, points);
		statement.setInt(5, historicalDownload);
		statement.setInt(6, dailyDownload);
		statement.setInt(7, dailyUniqueDownload);
		statement.setInt(8, dailyTwitchAppDownload);
		statement.setInt(9, dailyCurseForgeDownload);
		return statement;
	}

	@Override
	public int hashCode()
	{
		return id.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if(obj == null) return false;
		if(obj == this) return true;
		if(obj.getClass() != getClass()) return false;
		return id.equals(((Analytics) obj).id);
	}
}
