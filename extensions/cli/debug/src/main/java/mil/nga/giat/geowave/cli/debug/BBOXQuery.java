package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

@GeowaveOperation(name = "bbox", parentOperation = DebugSection.class)
@Parameters(commandDescription = "bbox query")
public class BBOXQuery extends
		AbstractGeoWaveQuery
{
	@Parameter(names = "--east, -e", required = true, description = "Max Longitude of BBOX")
	private Double east;

	@Parameter(names = "--west, -w", required = true, description = "Min Longitude of BBOX")
	private Double west;

	@Parameter(names = "--north, -n", required = true, description = "Max Latitude of BBOX")
	private Double north;

	@Parameter(names = "--south, -s", required = true, description = "Min Latitude of BBOX")
	private Double south;

	@Parameter(names = "--useAggregation, -agg", required = false, description = "Compute count on the server side")
	private Boolean useAggregation = Boolean.FALSE;

	private Geometry geom;

	private void getBoxGeom() {
		geom = new GeometryFactory().toGeometry(new Envelope(
				west,
				east,
				south,
				north));
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		getBoxGeom();

		long count = 0;
		if (useAggregation) {
			final QueryOptions options = new QueryOptions(
					adapterId,
					indexId);
			options.setAggregation(
					new CountAggregation(),
					adapter);
			try (final CloseableIterator<Object> it = dataStore.query(
					options,
					new SpatialQuery(
							geom))) {
				final CountResult result = ((CountResult) (it.next()));
				if (result != null) {
					count += result.getCount();
				}
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}
		else {
			try (final CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					new SpatialQuery(
							geom))) {
				while (it.hasNext()) {
					if (debug) {
						System.out.println(it.next());
					}
					else {
						it.next();
					}
					count++;
				}
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}
		return count;
	}

}
