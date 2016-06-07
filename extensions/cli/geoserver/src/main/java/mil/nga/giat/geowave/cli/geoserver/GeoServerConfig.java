package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

public class GeoServerConfig
{
	public static final String GEOSERVER_URL = "geoserver.url";
	public static final String GEOSERVER_USER = "geoserver.user";
	public static final String GEOSERVER_PASS = "geoserver.pass";
	public static final String GEOSERVER_WORKSPACE = "geoserver.workspace";

	public static final String DEFAULT_URL = "http://localhost:8080";
	public static final String DEFAULT_USER = "admin";
	public static final String DEFAULT_PASS = "geoserver";
	public static final String DEFAULT_WORKSPACE = "geowave";

	public final static String DISPLAY_NAME_PREFIX = "GeoWave Datastore - ";
	public static final String QUERY_INDEX_STRATEGY_KEY = "Query Index Strategy";

	private String url = DEFAULT_URL;
	private String user = DEFAULT_USER;
	private String pass = DEFAULT_PASS;
	private String workspace = DEFAULT_WORKSPACE;

	/**
	 * No-arg constructor uses defaults
	 */
	public GeoServerConfig() {}

	/**
	 * Properties File holds defaults; updates config if empty.
	 * 
	 * @param propFile
	 */
	public GeoServerConfig(
			File propFile ) {
		Properties gsConfig = ConfigOptions.loadProperties(
				propFile,
				null);

		boolean update = false;

		String geoserverUrl = gsConfig.getProperty(GEOSERVER_URL);
		if (geoserverUrl == null) {
			url = DEFAULT_URL;
			gsConfig.setProperty(
					GEOSERVER_URL,
					url);
			update = true;
		}

		String geoserverUser = gsConfig.getProperty(GEOSERVER_USER);
		if (geoserverUser == null) {
			user = DEFAULT_USER;
			gsConfig.setProperty(
					GEOSERVER_USER,
					user);
			update = true;
		}

		String geoserverPass = gsConfig.getProperty(GEOSERVER_PASS);
		if (geoserverPass == null) {
			pass = DEFAULT_PASS;
			gsConfig.setProperty(
					GEOSERVER_PASS,
					pass);
			update = true;
		}

		String geoserverWorkspace = gsConfig.getProperty(GEOSERVER_WORKSPACE);
		if (geoserverWorkspace == null) {
			workspace = DEFAULT_WORKSPACE;
			gsConfig.setProperty(
					GEOSERVER_WORKSPACE,
					workspace);
			update = true;
		}

		if (update) {
			ConfigOptions.writeProperties(
					propFile,
					gsConfig);

			System.out.println("GeoServer Config Saved");
		}
	}

	/**
	 * Secondary constructor for direct-access testing
	 * 
	 * @param url
	 * @param user
	 * @param pass
	 * @param workspace
	 */
	public GeoServerConfig(
			final String url,
			final String user,
			final String pass,
			final String workspace ) {
		this.url = url;
		this.user = user;
		this.pass = pass;
		this.workspace = workspace;
	}

	public HashMap<String, String> loadStoreConfig(
			File propFile,
			String datastore ) {
		Properties gsConfig = ConfigOptions.loadProperties(
				propFile,
				null);

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

	public String getUrl() {
		return url;
	}

	public void setUrl(
			String url ) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(
			String user ) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(
			String pass ) {
		this.pass = pass;
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			String workspace ) {
		this.workspace = workspace;
	}
}
