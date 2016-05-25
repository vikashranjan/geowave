package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

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

	public List<String> getWorkspaces() {
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

			List<String> workspaceList = new ArrayList<String>();

			for (int i = 0; i < workspaceArray.size(); i++) {
				workspaceList.add(workspaceArray.getJSONObject(
						i).getString(
						"name"));
			}

			return workspaceList;
		}

		return null;
	}

	public boolean addWorkspace(
			final String workspace ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		Response response = target.path(
				"geoserver/rest/workspaces").request().post(
				Entity.entity(
						"{'workspace':{'name':'" + workspace + "'}}",
						MediaType.APPLICATION_JSON));

		return response.getStatus() == 200;
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
				jsonObj = tempArray.getJSONObject(0);
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

	public static void main(
			final String[] args ) {
		GeoServerRestClient geoserverClient = new GeoServerRestClient(
				"http://localhost:8080",
				null,
				null,
				null);

		List<String> workspaceList = geoserverClient.getWorkspaces();

		System.out.println("\nList of GeoServer workspaces:");

		for (String ws : workspaceList) {
			System.out.println("  > " + ws);
		}

		System.out.println("---\n");
	}
}
