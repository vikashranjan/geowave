package mil.nga.giat.geowave.cli.geoserver;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "geoserver", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands that manage geoserver data stores and layers for geowave")
public class GeoServerSection extends
		DefaultOperation
{

}
