package brightspark.modanalytics.db;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;
import java.util.Map;

public abstract class DbStorable
{
	public abstract String getTableName();

	public abstract void getData(Map<String, Object> data);

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
