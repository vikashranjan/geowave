package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.List;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listworkspaces", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List the available GeoServer workspaces")
public class GeoServerGetWorkspacesCommand implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AddGeoServerCommand.class);
	private GeoServerRestClient geoserverClient;

	@Override
	public boolean prepare(
			OperationParams params ) {
		// Get the local config for GeoServer
		File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties gsConfig = ConfigOptions.loadProperties(
				propFile,
				null);

		// Create a rest client
		geoserverClient = new GeoServerRestClient(
				gsConfig.getProperty("geoserver.url"),
				null,
				null,
				null);
		
		// Successfully prepared (none needed).
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		List<String> workspaceList = geoserverClient.getWorkspaces();
		
		System.out.println("List of GeoServer workspaces:\n");
		
		for (String ws : workspaceList) {
			System.out.println("  > " + ws + "\n");
		}
	}
}
