package brightspark.modanalytics.dao;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AnalyticDate
{
	private final String date;
	private final int day;
	private final int month;
	private final int year;

	public AnalyticDate(ResultSet resultSet) throws SQLException
	{
		date = resultSet.getString("date");
		String[] components = date.split("/");
		day = Integer.parseInt(components[0]);
		month = Integer.parseInt(components[1]);
		year = Integer.parseInt(components[2]);
	}

	public String getDate()
	{
		return date;
	}

	public int getDay()
	{
		return day;
	}

	public int getMonth()
	{
		return month;
	}

	public int getYear()
	{
		return year;
	}

	@Override
	public String toString()
	{
		return date;
	}

	@Override
	public int hashCode()
	{
		return new HashCodeBuilder()
			.append(day)
			.append(month)
			.append(year)
			.toHashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if(obj == null) return false;
		if(obj == this) return true;
		if(obj.getClass() != getClass()) return false;
		AnalyticDate date = (AnalyticDate) obj;
		return new EqualsBuilder()
			.append(day, date.day)
			.append(month, date.month)
			.append(year, date.year)
			.isEquals();
	}
}
