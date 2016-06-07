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
		GeoServerConfigStoreCommand.class,
		GeoServerListStoresCommand.class,
		GeoServerGetStoreCommand.class,
		GeoServerAddStoreCommand.class,
		GeoServerRemoveStoreCommand.class,
		GeoServerListLayersCommand.class,
		GeoServerGetLayerCommand.class,
		GeoServerAddLayerCommand.class,
		GeoServerRemoveLayerCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
