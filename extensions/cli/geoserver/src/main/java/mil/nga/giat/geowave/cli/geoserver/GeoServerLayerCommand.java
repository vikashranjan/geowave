package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
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
	private String workspace = null;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = false, description = "Datastore Name")
	private String datastore = null;

	@Parameter(names = {
		"-n",
		"--name"
	}, required = false, description = "Layer Name")
	private String layerName = null;

	@Parameter(names = {
		"-a",
		"--action"
	}, required = false, description = "Datastore Action (get, add, delete, or list)")
	private String action;

	@Parameter(names = {
		"-g",
		"--geowaveOnly"
	}, required = false, description = "For list action: show only geowave layers (default: false)")
	private Boolean geowaveOnly = false;

	@Parameter(names = {
			"-debug"
		}, required = false, description = "Debug console output enabled")
		private Boolean debug = false;

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
			debugOut("Layer command prep - failed!");
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

		debugOut("Layer command: workspace = " + workspace + "; action = " + action);

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		debugDump(params);
		
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
		Response listLayersResponse = geoserverClient.getLayers(
				workspace,
				datastore,
				geowaveOnly);

		if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer layer list:");
			JSONObject listObj = JSONObject.fromObject(listLayersResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer layer list; code = " + listLayersResponse.getStatus());
		}
	}

	private void addLayer() {
		Response addLayerResponse = geoserverClient.addLayer(
				workspace,
				datastore,
				"defaultStyle",
				layerName);

		if (addLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer add layer response " + layerName + ":");
			JSONObject listObj = JSONObject.fromObject(addLayerResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err.println("Error adding GeoServer layer " + layerName + "; code = " + addLayerResponse.getStatus());
		}
	}

	private void deleteLayer() {
		Response deleteLayerResponse = geoserverClient.deleteLayer(
				layerName);

		if (deleteLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer delete layer response " + layerName + ":");
			JSONObject listObj = JSONObject.fromObject(deleteLayerResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err.println("Error deleting GeoServer layer " + layerName + "; code = " + deleteLayerResponse.getStatus());
		}
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
	
	private void debugOut(String message) {
		if (debug) {
			System.out.println("DEBUG: " + message);
		}
	}
	
	private void debugDump(
			OperationParams params ) {
		debugOut("DEBUG: Dump input params");
		GeoServerLayerCommand layerCommand = (GeoServerLayerCommand)params.getOperationMap().get("layer");
		
		debugOut("Action: " + layerCommand.getAction());
		debugOut("Workspace: " + layerCommand.getWorkspace());
		debugOut("Datastore: " + layerCommand.getDatastore());
	}
}
