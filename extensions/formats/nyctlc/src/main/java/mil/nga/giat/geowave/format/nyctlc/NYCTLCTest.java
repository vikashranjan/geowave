package mil.nga.giat.geowave.format.nyctlc;

import com.vividsolutions.jts.io.WKTReader;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.format.nyctlc.adapter.NYCTLCDataAdapter;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import mil.nga.giat.geowave.format.nyctlc.query.NYCTLCAggregation;
import mil.nga.giat.geowave.format.nyctlc.query.NYCTLCQuery;
import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCStatistics;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

import java.util.Date;

public class NYCTLCTest
{

	public static void main(
			String[] args )
			throws Exception {
		// normalIndexTest();
		newIndexTest();
	}

	public static void normalIndexTest()
			throws Exception {

		final AccumuloOperations operations = new BasicAccumuloOperations(
				"localhost:2181",
				"geowave",
				"root",
				"geowave",
				"spatial");

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				operations);

		IndexWriter<SimpleFeature> featureWriter = dataStore.createWriter(
				new FeatureDataAdapter(
						new NYCTLCIngestPlugin().getTypes()[0]),
				new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex());

		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				new NYCTLCIngestPlugin().getTypes()[0]);

		builder.set(
				NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName(),
				new WKTReader().read("POINT(-73.961990356445313 40.715862274169922)"));
		builder.set(
				NYCTLCUtils.Field.TIME_OF_DAY_SEC.getIndexedName(),
				0);

		featureWriter.write(builder.buildFeature(null));

		final QueryOptions queryOptions = new QueryOptions();

		queryOptions.setIndex(new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex());

		final Query query = new SpatialTemporalQuery(
				new Date(
						0),
				new Date(
						5),
				new WKTReader().read("POINT(-73.961990356445313 40.715862274169922)"));

		final CloseableIterator<SimpleFeature> results = dataStore.query(
				queryOptions,
				query);

		while (results.hasNext()) {
			final SimpleFeature feature = results.next();

			System.out.println(feature);
		}

		results.close();
	}

	public static void newIndexTest()
			throws Exception {

		final AccumuloOperations operations = new BasicAccumuloOperations(
				"10.0.0.55:2181",
				"accumulo",
				"foss4g",
				"foss4g",
				"foss4g.nyctlc");

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				operations);

		final QueryOptions queryOptions = new QueryOptions();

		queryOptions.setIndex(new NYCTLCDimensionalityTypeProvider().createPrimaryIndex());

		final Query query = new NYCTLCQuery(
				0,
				100,
				new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"),
				new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"));

		queryOptions.setAggregation(new CountAggregation<>(),
				new NYCTLCDataAdapter(
						new NYCTLCIngestPlugin().getTypes()[0]));

		if (queryOptions.getAggregation() != null) {
			final CloseableIterator<CountResult> results = dataStore.query(
					queryOptions,
					query);

			while (results.hasNext()) {
				final CountResult stats = results.next();

				System.out.println(stats.getCount());
			}
			results.close();
		}
		else {
			final CloseableIterator<SimpleFeature> results = dataStore.query(
					queryOptions,
					query);

			while (results.hasNext()) {
				final SimpleFeature feature = results.next();

				NYCTLCStatistics stats = new NYCTLCStatistics();
				stats.updateStats(feature);
				byte[] statsBytes = stats.toBinary();

				NYCTLCStatistics statsFromBytes = new NYCTLCStatistics();
				statsFromBytes.fromBinary(statsBytes);

				System.out.println(stats.equals(statsFromBytes));

				System.out.println(feature);
			}
			results.close();
		}
	}
}