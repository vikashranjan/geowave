package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "getsa", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get GeoWave store adapters")
public class GeoServerGetStoreAdapterCommand implements
		Command
{
	private GeoServerRestClient geoserverClient = null;
	private DataStorePluginOptions inputStoreOptions = null;

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();
	private String storeName = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		if (geoserverClient == null) {
			// Get the local config for GeoServer
			File propFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			GeoServerConfig config = new GeoServerConfig(
					propFile);

			// Create the rest client
			geoserverClient = new GeoServerRestClient(
					config);
		}

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <store name>");
		}

		storeName = parameters.get(0);

		if (inputStoreOptions == null) {
			inputStoreOptions = geoserverClient.getDataStorePlugin(
					storeName,
					geoserverClient.getConfig().getPropFile());
		}

		AdapterStore adapterStore = inputStoreOptions.createAdapterStore();
		
		System.out.println("Store " + storeName + " has these adapters:");
		
		try (final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {			
			while (it.hasNext()) {
				final DataAdapter<?> adapter = it.next();
				ByteArrayId adapterId = adapter.getAdapterId();
				
				System.out.println(adapterId.toString());
			}
			
		}
		catch (final IOException e) {
			System.err.println(
					"unable to close adapter iterator while looking up coverage names");
		}		
	}
}
