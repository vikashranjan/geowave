package mil.nga.giat.geowave.examples.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of
 * a few GeoTools queries against GeoWave. For simplicity, a MiniAccumuloCluster
 * is spun up and a few points from the DC area are ingested (Washington
 * Monument, White House, FedEx Field). Two queries are executed against this
 * data set.
 */
public class GeotoolsPerfQueryExample
{
	private static File tempAccumuloDir;
	private static MiniAccumuloCluster accumulo;
	private static DataStore dataStore;

	private static final PrimaryIndex index = IndexType.SPATIAL_VECTOR.createDefaultIndex();



	final static FeatureDataAdapter ADAPTER = new FeatureDataAdapter(
			getPointSimpleFeatureType());

	public static void main(
			String[] args )
			throws AccumuloException,
			AccumuloSecurityException,
			InterruptedException,
			IOException {

		// spin up a MiniAccumuloCluster and initialize the DataStore
		setup();

		// ingest 3 points represented as SimpleFeatures: Washington Monument,
		// White House, FedEx Field
		ingestCannedData();

		// execute a query for a bounding box
		executeBoundingBoxQuery();

		// stop MiniAccumuloCluster and delete temporary files
		cleanup();
	}

	private static void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			InterruptedException {

		final String ACCUMULO_USER = "root";
		final String ACCUMULO_PASSWORD = "Ge0wave";
		final String TABLE_NAMESPACE = "";

		tempAccumuloDir = Files.createTempDir();

		accumulo = new MiniAccumuloCluster(
				new MiniAccumuloConfig(
						tempAccumuloDir,
						ACCUMULO_PASSWORD));

		accumulo.start();

		dataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumulo.getZooKeepers(),
						accumulo.getInstanceName(),
						ACCUMULO_USER,
						ACCUMULO_PASSWORD,
						TABLE_NAMESPACE));
	}

	private static void ingestCannedData()
			throws IOException {

		final List<SimpleFeature> points = new ArrayList<>();

		System.out.println("Building SimpleFeatures from canned data set...");

		Random r  = new Random(57847);
		for(int i = 0; i < 10000;i++) {
			points.add(buildSimpleFeature(
					UUID.randomUUID().toString(),
					new Coordinate(r.nextGaussian()*10,r.nextGaussian()*10)));
		}

		System.out.println("Ingesting canned data...");

		try (IndexWriter indexWriter = dataStore.createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			for (SimpleFeature sf : points) {
				//
				indexWriter.write(
						ADAPTER,
						sf);

			}
		}

		System.out.println("Ingest complete.");
	}

	private static void executeBoundingBoxQuery()
			throws IOException {

		System.out.println("Constructing bounding box for the area contained by [Baltimore, MD and Richmond, VA.");

		int count = 0;
		final Geometry boundingBox = GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
				new Coordinate(-10,-10),
				new Coordinate(10,10)));

		System.out.println("Executing query, expecting to match ALL points...");

		long s = System.currentTimeMillis();
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				new QueryOptions(
						index),
				new SpatialQuery(
						boundingBox))) {

			while (iterator.hasNext()) {
				count++;
			}
		}
		System.out.println(count + " : " + (System.currentTimeMillis()-s));

	}


	private static void cleanup()
			throws IOException,
			InterruptedException {

		try {
			accumulo.stop();
		}
		finally {
			FileUtils.deleteDirectory(tempAccumuloDir);
		}
	}

	private static SimpleFeatureType getPointSimpleFeatureType() {

		final String NAME = "PointSimpleFeatureType";
		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
		sftBuilder.setName(NAME);
		sftBuilder.add(atBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"locationName"));
		sftBuilder.add(atBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"AttX"));
		sftBuilder.add(atBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"AttY"));
		sftBuilder.add(atBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));

		return sftBuilder.buildFeatureType();
	}

	private static SimpleFeature buildSimpleFeature(
			String locationName,
			Coordinate coordinate ) {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				getPointSimpleFeatureType());
		builder.set(
				"locationName",
				locationName);
		builder.set(
				"AttX",
				UUID.randomUUID().toString());
		builder.set(
				"AttY",
				UUID.randomUUID().toString());
		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));

		return builder.buildFeature(locationName);
	}

}
