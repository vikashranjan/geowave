package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "deleteworkspace", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Delete a GeoServer workspace")
public class GeoServerDeleteWorkspaceCommand implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoServerDeleteWorkspaceCommand.class);
	private GeoServerRestClient geoserverClient;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = true, description = "Workspace Name")
	private String workspace;

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
		boolean success = geoserverClient.deleteWorkspace(workspace);
		System.out.println("Delete workspace " + workspace + " from GeoServer: " + (success ? "OK" : "Failed"));
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			String workspace ) {
		this.workspace = workspace;
	}
}
