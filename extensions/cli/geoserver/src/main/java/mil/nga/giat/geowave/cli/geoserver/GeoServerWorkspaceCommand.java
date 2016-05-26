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

@GeowaveOperation(name = "workspace", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "GeoServer workspace CRUD")
public class GeoServerWorkspaceCommand implements
		Command
{
	private GeoServerRestClient geoserverClient;

	@Parameter(names = {
		"-n",
		"--name"
	}, required = false, description = "Workspace Name")
	private String name;

	@Parameter(names = {
		"-a",
		"--action"
	}, required = false, description = "Workspace Action (add, delete or list)")
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
		else if (action.equals("add") || action.startsWith("del")) {
			if (name != null && !name.isEmpty()) {
				valid = true;
			}
			else {
				System.err.println("You must supply a workspace name!");
			}
		}
		else if (action.startsWith("lis")) {
			valid = true;
		}

		if (!valid) {
			return false;
		}

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
				name);

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		if (action.equals("add")) {
			addWorkspace();
		}
		else if (action.startsWith("del")) {
			deleteWorkspace();
		}
		else {
			listWorkspaces();
		}
	}

	private void listWorkspaces() {
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
	}

	private void addWorkspace() {
		Response addWorkspaceResponse = geoserverClient.addWorkspace(name);
		if (addWorkspaceResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Add workspace '" + name + "' to GeoServer: OK");
		}
		else {
			System.err.println("Error adding workspace '" + name + "' to GeoServer; code = " + addWorkspaceResponse.getStatus());
		}
	}

	private void deleteWorkspace() {
		Response deleteWorkspaceResponse = geoserverClient.deleteWorkspace(name);
		if (deleteWorkspaceResponse.getStatus() == Status.CREATED.getStatusCode()) {
			System.out.println("Delete workspace '" + name + "' from GeoServer: OK");
		}
		else {
			System.err.println("Error deleting workspace '" + name + "' from GeoServer; code = " + deleteWorkspaceResponse.getStatus());
		}
	}

	public String getName() {
		return name;
	}

	public void setName(
			String workspace ) {
		this.name = workspace;
	}

	public String getAction() {
		return action;
	}

	public void setAction(
			String action ) {
		this.action = action;
	}
}
