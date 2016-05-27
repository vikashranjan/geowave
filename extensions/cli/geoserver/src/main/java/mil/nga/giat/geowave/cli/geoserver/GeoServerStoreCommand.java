package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.HashMap;
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

@GeowaveOperation(name = "store", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "GeoServer Store CRUD")
public class GeoServerStoreCommand implements
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
		"-n",
		"--name"
	}, required = false, description = "Store Name")
	private String datastore;

	@Parameter(names = {
		"-a",
		"--action"
	}, required = false, description = "Store Action (get, add, delete, config or list)")
	private String action;

	@Parameter(names = {
		"-u",
		"--user"
	}, required = false, description = "Store Config:User")
	private String storeUser;

	@Parameter(names = {
		"-p",
		"--password"
	}, required = false, description = "Store Config:Password")
	private String storePassword;

	@Parameter(names = {
		"-z",
		"--zookeeper"
	}, required = false, description = "Store Config:Zookeeper")
	private String storeZookeeper;

	@Parameter(names = {
		"-i",
		"--instance"
	}, required = false, description = "Store Config:Instance")
	private String storeInstance;

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
			if (datastore != null && !datastore.isEmpty()) {
				valid = true;
			}
			else {
				System.err.println("You must supply a store name!");
			}
		}
		else if (action.equals("list")) {
			valid = true;
		}
		else if (action.equals("config")) {
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
			getStore();
		}
		else if (action.equals("add")) {
			addStore();
		}
		else if (action.equals("delete")) {
			deleteStore();
		}
		else if (action.equals("config")) {
			configStore();
		}
		else {
			listStores();
		}
	}

	private void getStore() {
		Response getStoreResponse = geoserverClient.getDatastore(
				workspace,
				datastore);

		if (getStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer store info for '" + datastore + "':");

			JSONObject jsonResponse = JSONObject.fromObject(getStoreResponse.getEntity());
			JSONObject datastore = jsonResponse.getJSONObject("dataStore");
			System.out.println(datastore.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer store info for '" + datastore + "'; code = " + getStoreResponse.getStatus());
		}
	}

	private void listStores() {
		Response listStoresResponse = geoserverClient.getDatastores(workspace);

		if (listStoresResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer stores list for '" + workspace + "':");

			JSONObject jsonResponse = JSONObject.fromObject(listStoresResponse.getEntity());
			JSONArray datastores = jsonResponse.getJSONArray("dataStores");
			System.out.println(datastores.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer stores list for '" + workspace + "'; code = " + listStoresResponse.getStatus());
		}
	}

	private void addStore() {
		HashMap<String, String> geowaveStoreConfig = loadStoreConfig();

		Response addStoreResponse = geoserverClient.addDatastore(
				workspace,
				datastore,
				"accumulo",
				geowaveStoreConfig);

		if (addStoreResponse.getStatus() == Status.OK.getStatusCode() || addStoreResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add store '" + datastore + "' to workspace '" + workspace + "' on GeoServer: OK");
		}
		else {
			System.err.println("Error adding store '" + datastore + "' to workspace '" + workspace + "' on GeoServer; code = " + addStoreResponse.getStatus());
		}
	}

	private void deleteStore() {
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

	public void configStore() {
		boolean update = false;

		if (storeUser != null && !storeUser.isEmpty()) {
			gsConfig.setProperty(
					"geoserver.store.user",
					storeUser);
			System.out.println("GeoServer Store Config: User = " + storeUser);
			update = true;
		}

		if (storePassword != null && !storePassword.isEmpty()) {
			gsConfig.setProperty(
					"geoserver.store.password",
					storePassword);
			System.out.println("GeoServer Store Config: Password = " + storePassword);
			update = true;
		}

		if (storeZookeeper != null && !storeZookeeper.isEmpty()) {
			gsConfig.setProperty(
					"geoserver.store.zookeeper",
					storeZookeeper);
			System.out.println("GeoServer Store Config: Zookeeper = " + storeZookeeper);
			update = true;
		}

		if (storeInstance != null && !storeInstance.isEmpty()) {
			gsConfig.setProperty(
					"geoserver.store.instance",
					storeInstance);
			System.out.println("GeoServer Store Config: Instance = " + storeInstance);
			update = true;
		}

		if (update) {
			ConfigOptions.writeProperties(
					propFile,
					gsConfig);

			System.out.println("GeoServer Store Config Saved");
		}
		else {
			System.err.println("GeoServer store config not changed.");
			System.err.println("Please specify user, password, zookeeper, or instance");
		}
	}

	protected HashMap<String, String> loadStoreConfig() {
		HashMap<String, String> geowaveStoreConfig = new HashMap<String, String>();

		String sUser = gsConfig.getProperty(
				"geoserver.store.user",
				"root");
		String sPassword = gsConfig.getProperty(
				"geoserver.store.password",
				"password");
		String sZookeeper = gsConfig.getProperty(
				"geoserver.store.zookeeper",
				"localhost:2181");
		String sInstance = gsConfig.getProperty(
				"geoserver.store.instance",
				"geowave");

		geowaveStoreConfig.put(
				"user",
				sUser);
		geowaveStoreConfig.put(
				"password",
				sPassword);
		geowaveStoreConfig.put(
				"gwNamespace",
				datastore);
		geowaveStoreConfig.put(
				"zookeeper",
				sZookeeper);
		geowaveStoreConfig.put(
				"instance",
				sInstance);

		return geowaveStoreConfig;
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
