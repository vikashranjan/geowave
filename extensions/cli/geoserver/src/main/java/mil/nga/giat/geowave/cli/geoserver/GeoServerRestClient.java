package mil.nga.giat.geowave.cli.geoserver;

import java.io.IOException;
import java.util.Iterator;

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

		final Response resp = target
				.path(
						"geoserver/rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + ".json")
				.request()
				.get();

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
	
	public Response getDatastores(String workspaceName) {
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
		Response addWorkspaceResponse = geoserverClient.addWorkspace("DeleteMe");
		if (addWorkspaceResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add workspace 'DeleteMe' to GeoServer: OK");
		}
		else {
			System.err.println("Error adding workspace 'DeleteMe' to GeoServer; code = " + addWorkspaceResponse.getStatus());
		}

		// test deleteWorkspace
		Response deleteWorkspaceResponse = geoserverClient.deleteWorkspace("DeleteMe");
		if (deleteWorkspaceResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("Delete workspace 'DeleteMe' from GeoServer: OK");
		}
		else {
			System.err.println("Error deleting workspace 'DeleteMe' from GeoServer; code = " + deleteWorkspaceResponse.getStatus());
		}
		
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
		Response getStoreResponse = geoserverClient.getDatastore("topp", "taz_shapes");

		if (getStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer store info for 'topp/taz_shapes':");

			JSONObject jsonResponse = JSONObject.fromObject(getStoreResponse.getEntity());
			JSONObject datastore = jsonResponse.getJSONObject("dataStore");
			System.out.println(datastore.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer store info for 'topp/taz_shapes'; code = " + getStoreResponse.getStatus());
		}

	}
}
