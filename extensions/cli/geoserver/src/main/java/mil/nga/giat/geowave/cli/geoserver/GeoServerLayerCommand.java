package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "layer", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "GeoServer layer CRUD")
public class GeoServerLayerCommand implements
		Command
{
	private GeoServerRestClient geoserverClient;
	private File propFile;
	private Properties gsConfig;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private String workspace;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = false, description = "Datastore Name")
	private String datastore;

	@Parameter(names = {
		"-n",
		"--name"
	}, required = false, description = "Layer Name")
	private String layerName;

	@Parameter(names = {
		"-a",
		"--action"
	}, required = false, description = "Datastore Action (get, add, delete, or list)")
	private String action;

	@Override
	public boolean prepare(
			OperationParams params ) {
		// validate requested action:
		boolean valid = false;

		if (action == null || action.isEmpty()) {
			action = "list";
			valid = true;
		}
		else if (action.equals("get") || action.equals("add") || action.equals("delete")) {
			if (layerName != null && !layerName.isEmpty()) {
				valid = true;
			}
			else {
				System.err.println("You must supply a layer name!");
			}
		}
		else if (action.equals("list")) {
			valid = true;
		}

		if (!valid) {
			return false;
		}

		// Get the local config for GeoServer
		propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		gsConfig = ConfigOptions.loadProperties(
				propFile,
				null);

		// Create a rest client
		geoserverClient = new GeoServerRestClient(
				gsConfig.getProperty("geoserver.url"),
				gsConfig.getProperty(
						"geoserver.user",
						null),
				gsConfig.getProperty(
						"geoserver.password",
						null),
				workspace); // null is ok - uses default

		if (workspace == null || workspace.isEmpty()) { // retrieve it
			workspace = geoserverClient.getGeoserverWorkspace();
		}

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		if (action.equals("get")) {
			getLayer();
		}
		else if (action.equals("add")) {
			addLayer();
		}
		else if (action.equals("delete")) {
			deleteLayer();
		}
		else {
			listLayers();
		}
	}

	private void getLayer() {
		Response getLayerResponse = geoserverClient.getLayer(layerName);

		if (getLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer layer info for '" + layerName + "':");

			JSONObject jsonResponse = JSONObject.fromObject(getLayerResponse.getEntity());
			System.out.println(jsonResponse.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer layer info for '" + layerName + "'; code = " + getLayerResponse.getStatus());
		}
	}

	private void listLayers() {
	}

	private void addLayer() {
	}

	private void deleteLayer() {
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			String workspace ) {
		this.workspace = workspace;
	}

	public String getDatastore() {
		return datastore;
	}

	public void setDatastore(
			String datastore ) {
		this.datastore = datastore;
	}

	public String getAction() {
		return action;
	}

	public void setAction(
			String action ) {
		this.action = action;
	}
}
