package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "addcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer coverage store")
public class GeoServerAddCoverageStoreCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;
	private DataStorePluginOptions inputStoreOptions = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "<workspace name>")
	private String workspace;

	@Parameter(description = "<coverage store name>")
	private List<String> parameters = new ArrayList<String>();
	private String cvgstore = null;

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
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <coverage store name>");
		}

		cvgstore = parameters.get(0);

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		if (inputStoreOptions == null) {
			inputStoreOptions = geoserverClient.getDataStorePlugin(
					cvgstore,
					geoserverClient.getConfig().getPropFile());
		}

		// Get the store's accumulo config
		Map<String, String> storeConfigMap = inputStoreOptions.getFactoryOptionsAsMap();

		// Add in geoserver coverage store info
		storeConfigMap.put(
				GeoServerConfig.GEOSERVER_WORKSPACE,
				workspace);

		storeConfigMap.put(
				"geoserver.coverageStore",
				cvgstore);

		// storeConfigMap.put(
		// GeoServerConfig.GS_STORE_URL,
		// geoserverClient.getConfig().getStoreUrl());
		//
		// storeConfigMap.put(
		// GeoServerConfig.GS_STORE_PATH,
		// geoserverClient.getConfig().getStorePath());

		Response addStoreResponse = geoserverClient.addCoverageStore(storeConfigMap);

		if (addStoreResponse.getStatus() == Status.OK.getStatusCode() || addStoreResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add store '" + cvgstore + "' to workspace '" + workspace + "' on GeoServer: OK");
		}
		else {
			System.err.println("Error adding store '" + cvgstore + "' to workspace '" + workspace + "' on GeoServer; code = " + addStoreResponse.getStatus());
		}
	}
}
