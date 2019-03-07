package brightspark.modanalytics;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.io.File;

public class FileParamValidator implements IParameterValidator
{
	@Override
	public void validate(String name, String value) throws ParameterException
	{
		File file = new File(value);
		if(!file.exists() || !file.isFile())
			throw new ParameterException(String.format("The parameter '%s %s' does not point to a valid file location!", name, value));
	}
}
