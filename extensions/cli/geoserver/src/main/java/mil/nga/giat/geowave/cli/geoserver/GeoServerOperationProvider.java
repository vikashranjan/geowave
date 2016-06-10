package mil.nga.giat.geowave.cli.geoserver;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class GeoServerOperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		GeoServerSection.class,
		ConfigGeoServerCommand.class,
		GeoServerListWorkspacesCommand.class,
		GeoServerAddWorkspaceCommand.class,
		GeoServerRemoveWorkspaceCommand.class,
		GeoServerListDatastoresCommand.class,
		GeoServerGetDatastoreCommand.class,
		GeoServerAddDatastoreCommand.class,
		GeoServerRemoveDatastoreCommand.class,
		GeoServerListLayersCommand.class,
		GeoServerGetLayerCommand.class,
		GeoServerAddLayerCommand.class,
		GeoServerRemoveLayerCommand.class,
		GeoServerListCoverageStoresCommand.class,
		GeoServerGetCoverageStoreCommand.class,
		GeoServerAddCoverageStoreCommand.class,
		GeoServerRemoveCoverageStoreCommand.class,
		GeoServerListCoveragesCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
