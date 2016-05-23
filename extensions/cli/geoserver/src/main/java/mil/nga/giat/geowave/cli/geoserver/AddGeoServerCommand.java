package mil.nga.giat.geowave.cli.geoserver;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;

@GeowaveOperation(name = "addgeoserver", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for GeoServer")
public class AddGeoServerCommand implements
		Command
{

	@Override
	public boolean prepare(
			OperationParams arg0 ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void execute(
			OperationParams arg0 )
			throws Exception {
		// TODO Auto-generated method stub

	}

}
