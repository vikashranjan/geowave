package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

public class GeoServerRestClient
{
	private final static Logger logger = Logger.getLogger(GeoServerRestClient.class);
	private final static int defaultIndentation = 2;

	private static final String DEFAULT_URL = "http://localhost:8080";
	private static final String DEFAULT_USER = "admin";
	private static final String DEFAULT_PASS = "geoserver";
	private static final String DEFAULT_WORKSPACE = "geowave";

	public final static String DISPLAY_NAME_PREFIX = "GeoWave Datastore - ";
	public static final String QUERY_INDEX_STRATEGY_KEY = "Query Index Strategy";

	private final String geoserverUrl;
	private final String geoserverUser;
	private final String geoserverPass;
	private final String geoserverWorkspace;

	public GeoServerRestClient(
			String geoserverUrl,
			String geoserverUser,
			String geoserverPass,
			String geoserverWorkspace ) {
		if (geoserverUrl != null) {
			this.geoserverUrl = geoserverUrl;
		}
		else {
			this.geoserverUrl = DEFAULT_URL;
		}

		if (geoserverUser != null) {
			this.geoserverUser = geoserverUser;
		}
		else {
			this.geoserverUser = DEFAULT_USER;
		}

		if (geoserverPass != null) {
			this.geoserverPass = geoserverPass;
		}
		else {
			this.geoserverPass = DEFAULT_PASS;
		}

		if (geoserverWorkspace != null) {
			this.geoserverWorkspace = geoserverWorkspace;
		}
		else {
			this.geoserverWorkspace = DEFAULT_WORKSPACE;
		}
	}

	public Response getWorkspaces() {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/workspaces.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the workspace names
			final JSONArray workspaceArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"workspaces",
					"workspace");

			final JSONObject workspacesObj = new JSONObject();
			workspacesObj.put(
					"workspaces",
					workspaceArray);

			return Response.ok(
					workspacesObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	public Response addWorkspace(
			final String workspace ) {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"geoserver/rest/workspaces").request().post(
				Entity.entity(
						"{'workspace':{'name':'" + workspace + "'}}",
						MediaType.APPLICATION_JSON));
	}

	public Response deleteWorkspace(
			final String workspace ) {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"geoserver/rest/workspaces/" + workspace).queryParam(
				"recurse",
				"true").request().delete();
	}

	public Response getDatastore(
			final String workspaceName,
			String datastoreName ) {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			JSONObject datastore = JSONObject.fromObject(resp.readEntity(String.class));

			if (datastore != null) {
				return Response.ok(
						datastore.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	public Response getDatastores(
			String workspaceName ) {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the datastore names
			final JSONArray datastoreArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"dataStores",
					"dataStore");

			final JSONObject dsObj = new JSONObject();
			dsObj.put(
					"dataStores",
					datastoreArray);

			return Response.ok(
					dsObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	public Response addDatastore(
			String workspaceName,
			String datastoreName,
			String geowaveStoreType,
			Map<String, String> geowaveStoreConfig ) {
		String lockMgmt = "memory";
		String authMgmtPrvdr = "empty";
		String authDataUrl = "";
		String queryIndexStrategy = "Best Match";

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));

		final WebTarget target = client.target(geoserverUrl);

		final String dataStoreJson = createDatastoreJson(
				geowaveStoreType,
				geowaveStoreConfig,
				datastoreName,
				lockMgmt,
				authMgmtPrvdr,
				authDataUrl,
				queryIndexStrategy,
				true);

		// create a new geoserver style
		final Response resp = target.path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores").request().post(
				Entity.entity(
						dataStoreJson,
						MediaType.APPLICATION_JSON));

		if (resp.getStatus() == Status.CREATED.getStatusCode()) {
			return Response.ok().build();
		}

		return resp;
	}

	public Response deleteDatastore(
			String workspaceName,
			String datastoreName ) {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores/" + datastoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	public Response getLayer(
			final String layerName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/layers/" + layerName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			JSONObject layer = JSONObject.fromObject(resp.readEntity(String.class));

			if (layer != null) {
				return Response.ok(
						layer.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	/**
	 * Get list of layers from geoserver
	 * 
	 * @param workspaceName
	 *            : if null, don't filter on workspace
	 * @param datastoreName
	 *            : if null, don't filter on datastore
	 * @param geowaveOnly
	 *            : if true, only return geowave layers
	 * @return
	 */
	public Response getLayers(
			String workspaceName,
			String datastoreName,
			boolean geowaveOnly ) {
		boolean wsFilter = (workspaceName != null && !workspaceName.isEmpty());
		boolean dsFilter = (datastoreName != null && !datastoreName.isEmpty());

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/layers.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the datastore names
			final JSONArray layerArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"layers",
					"layer");

			// holder for simple layer info (when geowaveOnly = false)
			final JSONArray layerInfoArray = new JSONArray();

			final Map<String, List<String>> namespaceLayersMap = new HashMap<String, List<String>>();
			final Pattern p = Pattern.compile("workspaces/(.*?)/datastores/(.*?)/");
			for (int i = 0; i < layerArray.size(); i++) {
				boolean include = !geowaveOnly && !wsFilter && !dsFilter; // no filtering of any kind

				if (include) { // just grab it...
					layerInfoArray.add(layerArray.getJSONObject(i));
					continue; // and move on
				}

				// at this point, we are filtering somehow. get some more info about the layer
				final String name = layerArray.getJSONObject(
						i).getString(
						"name");

				final String layer = (String) getLayer(
						name).getEntity();

				// get the workspace and name for each datastore
				String ws = null;
				String ds = null;

				final Matcher m = p.matcher(layer);

				if (m.find()) {
					ws = m.group(1);
					ds = m.group(2);
				}

				// filter on datastore?
				if (!dsFilter || (ds != null && ds.equals(datastoreName))) {

					// filter on workspace?
					if (!wsFilter || (ws != null && ws.equals(workspaceName))) {
						final JSONObject datastore = JSONObject.fromObject(
								getDatastore(
										ds,
										ws).getEntity()).getJSONObject(
								"dataStore");

						// only process GeoWave layers
						if (geowaveOnly) {
							if (datastore != null && datastore.containsKey("type") && datastore.getString(
									"type").startsWith(
									"GeoWave Datastore")) {

								JSONArray entryArray = null;
								if (datastore.get("connectionParameters") instanceof JSONObject) {
									entryArray = datastore.getJSONObject(
											"connectionParameters").getJSONArray(
											"entry");
								}
								else if (datastore.get("connectionParameters") instanceof JSONArray) {
									entryArray = datastore.getJSONArray(
											"connectionParameters").getJSONObject(
											0).getJSONArray(
											"entry");
								}

								if (entryArray == null) {
									logger.error("entry Array is null - didn't find a connectionParameters datastore object that was a JSONObject or JSONArray");
								}
								else {
									// group layers by namespace
									for (int j = 0; j < entryArray.size(); j++) {
										final JSONObject entry = entryArray.getJSONObject(j);
										final String key = entry.getString("@key");
										final String value = entry.getString("$");

										if (key.startsWith("gwNamespace")) {
											if (namespaceLayersMap.containsKey(value)) {
												namespaceLayersMap.get(
														value).add(
														name);
											}
											else {
												final ArrayList<String> layers = new ArrayList<String>();
												layers.add(name);
												namespaceLayersMap.put(
														value,
														layers);
											}
											break;
										}
									}
								}
							}
						}
						else { // just get all the layers from this store
							layerInfoArray.add(layerArray.getJSONObject(i));
						}
					}
				}
			}

			// Handle geowaveOnly response
			if (geowaveOnly) {
				// create the json object with layers sorted by namespace
				final JSONArray layersArray = new JSONArray();
				for (final Map.Entry<String, List<String>> kvp : namespaceLayersMap.entrySet()) {
					final JSONArray layers = new JSONArray();

					for (int i = 0; i < kvp.getValue().size(); i++) {
						final JSONObject layerObj = new JSONObject();
						layerObj.put(
								"name",
								kvp.getValue().get(
										i));
						layers.add(layerObj);
					}

					final JSONObject layersObj = new JSONObject();
					layersObj.put(
							"namespace",
							kvp.getKey());
					layersObj.put(
							"layers",
							layers);

					layersArray.add(layersObj);
				}

				final JSONObject layersObj = new JSONObject();
				layersObj.put(
						"layers",
						layersArray);

				return Response.ok(
						layersObj.toString(defaultIndentation)).build();
			}
			else {
				final JSONObject layersObj = new JSONObject();
				layersObj.put(
						"layers",
						layerInfoArray);

				return Response.ok(
						layersObj.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	public Response addLayer(
			final String workspace,
			final String datastore,
			final String defaultStyle,
			final String layerName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		Response resp = target.path(
				"geoserver/rest/workspaces/" + workspace + "/datastores/" + datastore + "/featuretypes").request().post(
				Entity.entity(
						"{'name':'" + layerName + "'}",
						MediaType.APPLICATION_JSON));

		if (resp.getStatus() != Status.CREATED.getStatusCode()) {
			return resp;
		}

		resp = target.path(
				"geoserver/rest/layers/" + layerName).request().put(
				Entity.entity(
						"{'layer':{'defaultStyle':{'name':'" + defaultStyle + "'}}}",
						MediaType.APPLICATION_JSON));

		return resp;
	}

	// Internal methods
	protected String createFeatureTypeJson(
			final String featureTypeName ) {
		final JSONObject featTypeJson = new JSONObject();

		featTypeJson.put(
				"name",
				featureTypeName);

		final JSONObject jsonObj = new JSONObject();
		jsonObj.put(
				"featureType",
				featTypeJson);

		return jsonObj.toString();
	}

	protected JSONArray getArrayEntryNames(
			JSONObject jsonObj,
			final String firstKey,
			final String secondKey ) {
		// get the top level object/array
		if (jsonObj.get(firstKey) instanceof JSONObject) {
			jsonObj = jsonObj.getJSONObject(firstKey);
		}
		else if (jsonObj.get(firstKey) instanceof JSONArray) {
			final JSONArray tempArray = jsonObj.getJSONArray(firstKey);
			if (tempArray.size() > 0) {
				if (tempArray.get(0) instanceof JSONObject) {
					jsonObj = tempArray.getJSONObject(0);
				}
				else {
					// empty list!
					return new JSONArray();
				}
			}
		}

		// get the sub level object/array
		final JSONArray entryArray = new JSONArray();
		if (jsonObj.get(secondKey) instanceof JSONObject) {
			final JSONObject entry = new JSONObject();
			entry.put(
					"name",
					jsonObj.getJSONObject(
							secondKey).getString(
							"name"));
			entryArray.add(entry);
		}
		else if (jsonObj.get(secondKey) instanceof JSONArray) {
			final JSONArray entries = jsonObj.getJSONArray(secondKey);
			for (int i = 0; i < entries.size(); i++) {
				final JSONObject entry = new JSONObject();
				entry.put(
						"name",
						entries.getJSONObject(
								i).getString(
								"name"));
				entryArray.add(entry);
			}
		}
		return entryArray;
	}

	protected String createDatastoreJson(
			final String geowaveStoreType,
			final Map<String, String> geowaveStoreConfig,
			final String name,
			final String lockMgmt,
			final String authMgmtProvider,
			final String authDataUrl,
			final String queryIndexStrategy,
			final boolean enabled ) {
		final JSONObject dataStore = new JSONObject();
		dataStore.put(
				"name",
				name);
		dataStore.put(
				"type",
				DISPLAY_NAME_PREFIX + geowaveStoreType);
		dataStore.put(
				"enabled",
				Boolean.toString(enabled));

		final JSONObject connParams = new JSONObject();

		if (geowaveStoreConfig != null) {
			for (final Entry<String, String> e : geowaveStoreConfig.entrySet()) {
				connParams.put(
						e.getKey(),
						e.getValue());
			}
		}
		connParams.put(
				"Lock Management",
				lockMgmt);

		connParams.put(
				QUERY_INDEX_STRATEGY_KEY,
				queryIndexStrategy);

		connParams.put(
				"Authorization Management Provider",
				authMgmtProvider);
		if (!authMgmtProvider.equals("empty")) {
			connParams.put(
					"Authorization Data URL",
					authDataUrl);
		}

		dataStore.put(
				"connectionParameters",
				connParams);

		final JSONObject jsonObj = new JSONObject();
		jsonObj.put(
				"dataStore",
				dataStore);

		return jsonObj.toString();
	}

	// Example use of geoserver rest client
	public static void main(
			final String[] args ) {
		// create the client
		GeoServerRestClient geoserverClient = new GeoServerRestClient(
				"http://localhost:8080",
				null,
				null,
				null);

		// test getWorkspaces
		Response getWorkspacesResponse = geoserverClient.getWorkspaces();

		if (getWorkspacesResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nList of GeoServer workspaces:");

			JSONObject jsonResponse = JSONObject.fromObject(getWorkspacesResponse.getEntity());

			final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");
			for (int i = 0; i < workspaces.size(); i++) {
				String wsName = workspaces.getJSONObject(
						i).getString(
						"name");
				System.out.println("  > " + wsName);
			}

			System.out.println("---\n");
		}
		else {
			System.err.println("Error getting GeoServer workspace list; code = " + getWorkspacesResponse.getStatus());
		}

		// test addWorkspace
		// Response addWorkspaceResponse =
		// geoserverClient.addWorkspace("DeleteMe");
		// if (addWorkspaceResponse.getStatus() ==
		// Status.CREATED.getStatusCode()) {
		// System.out.println("Add workspace 'DeleteMe' to GeoServer: OK");
		// }
		// else {
		// System.err.println("Error adding workspace 'DeleteMe' to GeoServer; code = "
		// + addWorkspaceResponse.getStatus());
		// }

		// test store list
		Response listStoresResponse = geoserverClient.getDatastores("topp");

		if (listStoresResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer stores list for 'topp':");

			JSONObject jsonResponse = JSONObject.fromObject(listStoresResponse.getEntity());
			JSONArray datastores = jsonResponse.getJSONArray("dataStores");
			System.out.println(datastores.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer stores list for 'topp'; code = " + listStoresResponse.getStatus());
		}

		// test get store
		Response getStoreResponse = geoserverClient.getDatastore(
				"topp",
				"taz_shapes");

		if (getStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer store info for 'topp/taz_shapes':");

			JSONObject jsonResponse = JSONObject.fromObject(getStoreResponse.getEntity());
			JSONObject datastore = jsonResponse.getJSONObject("dataStore");
			System.out.println(datastore.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer store info for 'topp/taz_shapes'; code = " + getStoreResponse.getStatus());
		}

		/*
		 * // test add store HashMap<String, String> geowaveStoreConfig = new HashMap<String, String>();
		 * geowaveStoreConfig.put("user", "root"); geowaveStoreConfig.put("password", "password");
		 * geowaveStoreConfig.put("gwNamespace", "kamteststore2"); geowaveStoreConfig.put("zookeeper",
		 * "localhost:2181"); geowaveStoreConfig.put("instance", "geowave");
		 * 
		 * Response addStoreResponse = geoserverClient.addDatastore( "DeleteMe", "kamteststore2", "accumulo",
		 * geowaveStoreConfig);
		 * 
		 * if (addStoreResponse.getStatus() == Status.OK.getStatusCode() || addStoreResponse.getStatus() ==
		 * Status.CREATED.getStatusCode()) { System.out.println(
		 * "Add store 'kamstoretest2' to workspace 'DeleteMe' on GeoServer: OK" ); } else { System.err.println(
		 * "Error adding store 'kamstoretest2' to workspace 'DeleteMe' on GeoServer; code = " +
		 * addStoreResponse.getStatus()); }
		 * 
		 * // test delete store Response deleteStoreResponse = geoserverClient.deleteDatastore( "DeleteMe",
		 * "kamteststore");
		 * 
		 * if (deleteStoreResponse.getStatus() == Status.OK.getStatusCode() || addStoreResponse.getStatus() ==
		 * Status.CREATED.getStatusCode()) { System.out.println(
		 * "Delete store 'kamstoretest' from workspace 'DeleteMe' on GeoServer: OK" ); } else { System.err.println(
		 * "Error deleting store 'kamstoretest' from workspace 'DeleteMe' on GeoServer; code = " +
		 * deleteStoreResponse.getStatus()); }
		 * 
		 * // test deleteWorkspace Response deleteWorkspaceResponse = geoserverClient.deleteWorkspace("DeleteMe"); if
		 * (deleteWorkspaceResponse.getStatus() == Status.OK.getStatusCode()) {
		 * System.out.println("Delete workspace 'DeleteMe' from GeoServer: OK"); } else { System.err.println(
		 * "Error deleting workspace 'DeleteMe' from GeoServer; code = " + deleteWorkspaceResponse.getStatus()); }
		 */

		// test getLayer
		Response getLayerResponse = geoserverClient.getLayer("states");

		if (getLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer layer info for 'states':");

			JSONObject jsonResponse = JSONObject.fromObject(getLayerResponse.getEntity());
			System.out.println(jsonResponse.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer layer info for 'states'; code = " + getLayerResponse.getStatus());
		}

		// test list layers
		Response listLayersResponse = geoserverClient.getLayers(
				"topp",
				null,
				false);
		if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer layer list:");
			JSONObject listObj = JSONObject.fromObject(listLayersResponse.getEntity());
			System.out.println(listObj.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer layer list; code = " + listLayersResponse.getStatus());
		}
	}

	public String getGeoserverUrl() {
		return geoserverUrl;
	}

	public String getGeoserverUser() {
		return geoserverUser;
	}

	public String getGeoserverPass() {
		return geoserverPass;
	}

	public String getGeoserverWorkspace() {
		return geoserverWorkspace;
	}
}
