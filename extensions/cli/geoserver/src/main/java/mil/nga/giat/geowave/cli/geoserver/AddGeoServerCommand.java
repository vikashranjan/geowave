package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "addgeoserver", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for GeoServer")
public class AddGeoServerCommand implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AddGeoServerCommand.class);

	public static final String GEOSERVER_URL = "GEOWAVE_GEOSERVER_URL";

	@Parameter(names = {
			"-u",
			"--url"
		}, required = true, description = "GeoServer URL")
		private String url;

	@Override
	public boolean prepare(
			OperationParams params ) {
		// Successfully prepared (none needed).
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		existingProps.setProperty(
				GEOSERVER_URL,
				getUrl());

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
