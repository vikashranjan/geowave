package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rmds", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer DataStore")
public class GeoServerRemoveStoreCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = true, description = "Workspace Name")
	private String workspace;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = true, description = "DataStore Name")
	private String datastore;

	@Override
	public boolean prepare(
			OperationParams params ) {
		if (geoserverClient == null) {
			// Get the local config for GeoServer
			File propFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			GeoServerConfig config = new GeoServerConfig(
					propFile);

			// Create the rest client
			geoserverClient = new GeoServerRestClient(
					config);
		}

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		Response deleteStoreResponse = geoserverClient.deleteDatastore(
				workspace,
				datastore);

		if (deleteStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("Delete store '" + datastore + "' from workspace '" + workspace + "' on GeoServer: OK");
		}
		else {
			System.err.println("Error deleting store '" + datastore + "' from workspace '" + workspace + "' on GeoServer; code = " + deleteStoreResponse.getStatus());
		}
	}
}
