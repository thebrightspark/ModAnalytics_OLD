package brightspark.modanalytics;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.io.File;

public class DirParamValidator implements IParameterValidator
{
	@Override
	public void validate(String name, String value) throws ParameterException
	{
		File file = new File(value);
		if(!file.exists() || !file.isDirectory())
			throw new ParameterException(String.format("The parameter '%s %s' does not point to a valid directory location!", name, value));
	}
}
