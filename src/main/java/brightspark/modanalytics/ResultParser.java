package brightspark.modanalytics;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultParser<R>
{
	/**
	 * Applies this result parser to the given {@link ResultSet}
	 * @param results The results to parse
	 * @return The output object for the results
	 */
	R apply(ResultSet results) throws SQLException;
}
