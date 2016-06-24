package mil.nga.giat.geowave.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.cli.geoserver.GeoServerConfig;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRestClient;
import mil.nga.giat.geowave.service.GeoserverService;
import mil.nga.giat.geowave.service.ServiceUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Produces(MediaType.APPLICATION_JSON)
@Path("/geoserver")
public class GeoserverServiceImpl implements
		GeoserverService
{
	private final static Logger log = Logger.getLogger(GeoserverServiceImpl.class);
	private final static int defaultIndentation = 2;

	private final GeoServerRestClient geoserverClient;

	private final String geoserverUrl;
	private final String geoserverUser;
	private final String geoserverPass;
	private final String defaultWorkspace;

	public GeoserverServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		final Properties props = ServiceUtils.loadProperties(servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties")));

		geoserverUrl = ServiceUtils.getProperty(
				props,
				"geoserver.url");

		geoserverUser = ServiceUtils.getProperty(
				props,
				"geoserver.username");

		geoserverPass = ServiceUtils.getProperty(
				props,
				"geoserver.password");

		defaultWorkspace = ServiceUtils.getProperty(
				props,
				"geoserver.workspace");

		// init the gs config from servlet config
		GeoServerConfig gsConfig = new GeoServerConfig();
		gsConfig.setUrl(geoserverUrl);
		gsConfig.setUser(geoserverUser);
		gsConfig.setPass(geoserverPass);
		gsConfig.setWorkspace(defaultWorkspace);

		// create the rest client
		geoserverClient = new GeoServerRestClient(
				gsConfig);
	}

	@Override
	@GET
	@Path("/workspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getWorkspaces() {
		return geoserverClient.getWorkspaces();
	}

	private JSONArray getArrayEntryNames(
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

	@Override
	@POST
	@Path("/workspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createWorkspace(
			final FormDataMultiPart multiPart ) {

		final String workspace = multiPart.getField(
				"workspace").getValue();

		return geoserverClient.addWorkspace(workspace);
	}

	@Override
	@DELETE
	@Path("/workspaces/{workspace}")
	public Response deleteWorkspace(
			@PathParam("workspace")
			final String workspace ) {
		return geoserverClient.deleteWorkspace(workspace);
	}

	@Override
	@GET
	@Path("/styles")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStyles() {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/styles.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {

			resp.bufferEntity();

			// get the style names
			final JSONArray styleArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"styles",
					"style");

			final JSONObject stylesObj = new JSONObject();
			stylesObj.put(
					"styles",
					styleArray);

			return Response.ok(
					stylesObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	@Override
	@GET
	@Path("/styles/{styleName}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getStyle(
			@PathParam("styleName")
			final String styleName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/styles/" + styleName + ".sld").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			final InputStream inStream = (InputStream) resp.getEntity();

			return Response.ok(
					inStream,
					MediaType.APPLICATION_XML).header(
					"Content-Disposition",
					"attachment; filename=\"" + styleName + ".sld\"").build();
		}

		return resp;
	}

	@Override
	@POST
	@Path("/styles")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishStyle(
			final FormDataMultiPart multiPart ) {

		final Collection<FormDataBodyPart> fileFields = multiPart.getFields("file");
		if (fileFields == null) {
			return Response.noContent().build();
		}

		// read the list of files & upload to geoserver services
		for (final FormDataBodyPart field : fileFields) {
			final String filename = field.getFormDataContentDisposition().getFileName();
			if (filename.endsWith(".sld") || filename.endsWith(".xml")) {
				final String styleName = filename.substring(
						0,
						filename.length() - 4);
				final InputStream inStream = field.getValueAs(InputStream.class);

				final Client client = ClientBuilder.newClient().register(
						HttpAuthenticationFeature.basic(
								geoserverUser,
								geoserverPass));
				final WebTarget target = client.target(geoserverUrl);

				// create a new geoserver style
				target.path(
						"geoserver/rest/styles").request().post(
						Entity.entity(
								"{'style':{'name':'" + styleName + "','filename':'" + styleName + ".sld'}}",
								MediaType.APPLICATION_JSON));

				// upload the style to geoserver
				final Response resp = target.path(
						"geoserver/rest/styles/" + styleName).request().put(
						Entity.entity(
								inStream,
								"application/vnd.ogc.sld+xml"));

				if (resp.getStatus() != Status.OK.getStatusCode()) {
					return resp;
				}
			}
		}

		return Response.ok().build();
	}

	@Override
	@DELETE
	@Path("/styles/{styleName}")
	public Response deleteStyle(
			@PathParam("styleName")
			final String styleName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"geoserver/rest/styles/" + styleName).request().delete();
	}

	@Override
	@GET
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastores(
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		return geoserverClient.getDatastores(customWorkspace);
	}

	@Override
	@GET
	@Path("/gwdatastores")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGeoWaveDatastores(
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {
		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/workspaces/" + customWorkspace + "/datastores.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {

			resp.bufferEntity();

			// get the datastore names
			final JSONArray datastoreArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"dataStores",
					"dataStore");

			final JSONArray datastores = new JSONArray();
			for (int i = 0; i < datastoreArray.size(); i++) {
				final String name = datastoreArray.getJSONObject(
						i).getString(
						"name");

				final JSONObject dsObj = JSONObject.fromObject(
						getDatastore(
								name,
								customWorkspace).getEntity()).getJSONObject(
						"dataStore");

				// only process the GeoWave datastores
				if ((dsObj != null) && dsObj.containsKey("type") && dsObj.getString(
						"type").startsWith(
						"GeoWave Datastore")) {

					final JSONObject datastore = new JSONObject();

					datastore.put(
							"name",
							name);

					JSONArray entryArray = null;
					if (dsObj.get("connectionParameters") instanceof JSONObject) {
						entryArray = dsObj.getJSONObject(
								"connectionParameters").getJSONArray(
								"entry");
					}
					else if (dsObj.get("connectionParameters") instanceof JSONArray) {
						entryArray = dsObj.getJSONArray(
								"connectionParameters").getJSONObject(
								0).getJSONArray(
								"entry");
					}

					if (entryArray == null) {
						log.error("entry Array was null; didn't find a valid connectionParameters datastore object of type JSONObject or JSONArray");
					}
					else {
						// report connection params for each data store
						for (int j = 0; j < entryArray.size(); j++) {
							final JSONObject entry = entryArray.getJSONObject(j);
							final String key = entry.getString("@key");
							final String value = entry.getString("$");

							datastore.put(
									key,
									value);
						}
					}
					datastores.add(datastore);
				}
			}

			final JSONObject dsObj = new JSONObject();
			dsObj.put(
					"dataStores",
					datastores);

			return Response.ok(
					dsObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	@Override
	@GET
	@Path("/datastores/{datastoreName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastore(
			@PathParam("datastoreName")
			final String datastoreName,
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		return geoserverClient.getDatastore(
				customWorkspace,
				datastoreName);
	}

	@Override
	@POST
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishDatastore(
			final FormDataMultiPart multiPart ) {
		final Map<String, List<FormDataBodyPart>> fieldMap = multiPart.getFields();

		String customWorkspace = defaultWorkspace;
		String name = "geowave";

		for (final Entry<String, List<FormDataBodyPart>> e : fieldMap.entrySet()) {
			if ((e.getValue() != null) && !e.getValue().isEmpty()) {
				if (e.getKey().equals(
						"workspace")) {
					customWorkspace = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"name")) {
					name = e.getValue().get(
							0).getValue();
				}
			}
		}

		return geoserverClient.addDatastore(
				customWorkspace,
				null,
				name);
	}

	@Override
	@DELETE
	@Path("/datastores/{datastoreName}")
	public Response deleteDatastore(
			@PathParam("datastoreName")
			final String datastoreName,
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		return geoserverClient.deleteDatastore(
				customWorkspace,
				datastoreName);
	}

	@Override
	@GET
	@Path("/layers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLayers() {
		return geoserverClient.getFeatureLayers(
				null,
				null,
				true);
	}

	@Override
	@GET
	@Path("/layers/{layerName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLayer(
			@PathParam("layerName")
			final String layerName ) {
		return geoserverClient.getFeatureLayer(layerName);
	}

	@Override
	@POST
	@Path("/layers")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishLayer(
			final FormDataMultiPart multiPart ) {

		final String datastore = multiPart.getField(
				"datastore").getValue();

		final String customWorkspace = (multiPart.getField("workspace") != null) ? multiPart.getField(
				"workspace").getValue() : defaultWorkspace;

		String jsonString;
		try {
			jsonString = IOUtils.toString(multiPart.getField(
					"featureType").getValueAs(
					InputStream.class));
		}
		catch (final IOException e) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Layer Creation Failed - Unable to parse featureType").build());
		}

		final String layerName = JSONObject.fromObject(
				jsonString).getJSONObject(
				"featureType").getString(
				"name");

		return geoserverClient.addFeatureLayer(
				customWorkspace,
				datastore,
				layerName);
	}

	@Override
	@DELETE
	@Path("/layers/{layer}")
	public Response deleteLayer(
			@PathParam("layer")
			final String layerName ) {
		return geoserverClient.deleteFeatureLayer(layerName);
	}
}