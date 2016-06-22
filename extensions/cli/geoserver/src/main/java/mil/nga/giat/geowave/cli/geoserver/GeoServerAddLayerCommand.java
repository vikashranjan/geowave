package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import net.sf.json.JSONObject;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "addlayer", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer layer from the given GeoWave store")
public class GeoServerAddLayerCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
			"-ws",
			"--workspace"
		}, required = false, description = "<workspace name>")
		private String workspace = null;

	@Parameter(names = {
			"-a",
			"--addAll"
		}, required = false, description = "Add all layers for the given store")
		private Boolean addAll = false;

	@Parameter(description = "<GeoWave store name>")
	private List<String> parameters = new ArrayList<String>();
	private String gwStore = null;

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
					"Requires argument: <store name>");
		}

		gwStore = parameters.get(0);

		if (workspace == null || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		Response addLayerResponse = geoserverClient.addLayer(
				workspace,
				gwStore,
				addAll);

		if (addLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("Add GeoServer layer for '" + gwStore + ": OK");
			
			JSONObject jsonResponse = JSONObject.fromObject(addLayerResponse.getEntity());
			System.out.println(jsonResponse.toString(2));
		}
		else {
			System.err.println("Error adding GeoServer layer for store '" + gwStore + "; code = " + addLayerResponse.getStatus());
		}
	}
}
