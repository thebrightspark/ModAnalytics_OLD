package brightspark.modanalytics;

import brightspark.modanalytics.db.DbConnection;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Main
{
	private static final File CONFIG_FILE = new File("config.properties");

	private static Logger log = LogManager.getLogger(Main.class);
	private static DbConnection db;
	private static List<Integer> projectIds = new LinkedList<>();

	/*
	TODO: Try use Spring
	TODO: Try use org.springframework.scheduling.quartz.CronTriggerBean to get the CSVs once per day

	Example setup from ATS BR:

	<bean id="refreshBetradarEventsJob" class="org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean">
		<property name="targetObject" ref="feedService" />
		<property name="targetMethod" value="cronJob" />
		<property name="concurrent" value="false" />
	</bean>

	<bean id="refreshBetradarEventsTrigger" class="org.springframework.scheduling.quartz.CronTriggerBean">
		<property name="jobDetail" ref="refreshBetradarEventsJob" />
		<!-- Fire at 10pm, every day -->
		<property name="cronExpression" value="0 0 22 * * ?" />
		<property name="misfireInstructionName" value="MISFIRE_INSTRUCTION_DO_NOTHING" />
	</bean>

	<bean id="betradarCronRescheduler" class="ats.core.util.CronRescheduler">
		<property name="cronTrigger" ref="refreshBetradarEventsTrigger" />
		<property name="scheduler" ref="feedScheduler" />
	</bean>
	 */

	public static void main(String... args)
	{
		//TODO: Have a way to input the CSV files
		// Maybe check a certain directory every 10s and import (then delete/move) the CSVs into the DB
		init();
	}

	private static void init()
	{
		handleProperties();
		try
		{
			db = new DbConnection(null);
		}
		catch(SQLException e)
		{
			log.error("Couldn't connect to the DB", e);
		}
		catch(ClassNotFoundException e)
		{
			log.error("Couldn't initialise JDBC", e);
		}
	}

	private static void handleProperties()
	{
		Properties properties = new Properties();
		if(!CONFIG_FILE.exists())
		{
			//If not config file, then generate default and exit
			log.error("No config file found at {}. Generating default but user must enter project IDs into file.", CONFIG_FILE.getAbsolutePath());
			properties.setProperty("projectIds", "");
			try(OutputStream output = new FileOutputStream(CONFIG_FILE))
			{
				properties.store(output, null);
			}
			catch(IOException e)
			{
				log.error("Failed to create default config file at " + CONFIG_FILE.getAbsolutePath(), e);
			}
			return;
		}

		//Load properties
		try(InputStream input = new FileInputStream(CONFIG_FILE))
		{
			properties.load(input);
		}
		catch(IOException e)
		{
			log.error("Failed to load config file at " + CONFIG_FILE.getAbsolutePath(), e);
			System.exit(0);
		}

		//Read properties
		String projectIdsString = properties.getProperty("projectIds").replaceAll(" ", "");
		if(projectIdsString.isEmpty())
		{
			log.error("No project IDs defined in config file {}", CONFIG_FILE.getAbsolutePath());
		}
		for(String idString : projectIdsString.split(","))
		{
			try
			{
				projectIds.add(Integer.parseInt(idString));
			}
			catch(NumberFormatException e)
			{
				log.error("Couldn't parse project ID '{}' as an integer", idString);
			}
		}
		if(projectIds.isEmpty())
		{
			log.error("No project IDs successfully read from config file " + CONFIG_FILE.getAbsolutePath());
			System.exit(0);
		}

		log.info("Read project IDs: {}", projectIds);
	}

	private static void downloadLatestAnalytics() throws IOException
	{
		//TODO: Get CSV data
		String content;
		CloseableHttpClient client = HttpClients.createDefault();
		try
		{
			content = client.execute(new HttpGet("https://minecraft.curseforge.com/dashboard/project/237240/exportcsv"), response ->
			{
				int status = response.getStatusLine().getStatusCode();
				if(status >= 200 && status < 300)
				{
					HttpEntity entity = response.getEntity();
					return entity == null ? null : EntityUtils.toString(entity);
				}
				return null;
			});
		}
		finally
		{
			client.close();
		}
		System.out.println(content);
	}
}
