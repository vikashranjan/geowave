package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "configds", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Manage the GeoServer datastore configuration")
public class GeoServerConfigStoreCommand implements
		Command
{
	private File propFile;
	private Properties gsConfig;

	@Parameter(names = {
		"-u",
		"--user"
	}, required = true, description = "<username>")
	private String storeUser;

	@Parameter(names = {
		"-p",
		"--password"
	}, required = true, description = "<password>")
	private String storePassword;

	@Parameter(names = {
		"-z",
		"--zookeeper"
	}, required = true, description = "<zookeeper url>")
	private String storeZookeeper;

	@Parameter(names = {
		"-i",
		"--instance"
	}, required = true, description = "<instance>")
	private String storeInstance;

	@Override
	public boolean prepare(
			OperationParams params ) {
		// Get the local config for GeoServer
		propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		gsConfig = ConfigOptions.loadProperties(
				propFile,
				null);

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		gsConfig.setProperty(
				"geoserver.store.user",
				storeUser);

		gsConfig.setProperty(
				"geoserver.store.password",
				storePassword);

		gsConfig.setProperty(
				"geoserver.store.zookeeper",
				storeZookeeper);

		gsConfig.setProperty(
				"geoserver.store.instance",
				storeInstance);

		ConfigOptions.writeProperties(
				propFile,
				gsConfig);

		System.out.println("GeoServer DataStore Config updated");
	}
}
