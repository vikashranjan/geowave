package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.math.util.MathUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportCommand;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportOptions;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.operations.LocalToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCIngestPlugin;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.adapter.NYCTLCDataAdapter;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import mil.nga.giat.geowave.format.nyctlc.query.NYCTLCQuery;

public class GeoWaveNYCTLCIT extends
		GeoWaveTestEnvironment
{
	@Test
	public void testSingleThreadedIngestAndQuerySpatialPointsAndLines() throws ParseException, IOException {
		testIngestAndQuerySpatialPointsAndLines(1);
	}

	public void testIngestAndQuerySpatialPointsAndLines(
			final int nthreads ) throws ParseException, IOException {
		System.getProperties().put(
				"AccumuloIndexWriter.skipFlush",
				"true");

		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments

		// Ingest Formats
		IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin("nyctlc");

		// Indexes
		IndexPluginOptions indexOption = new IndexPluginOptions();
		indexOption.selectPlugin("spatial");

		// Create the command and execute.
		DataStorePluginOptions plugin = getAccumuloStorePluginOptions(TEST_NAMESPACE);
		LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
		localIngester.setPluginFormats(ingestFormatOptions);
		localIngester.setInputIndexOptions(Arrays.asList(indexOption));
		localIngester.setInputStoreOptions(plugin);
		localIngester.setParameters(
				"green_tripdata_2013-08.csv",
				null,
				null);
		localIngester.setThreads(nthreads);
		localIngester.execute(new ManualOperationParams());

		final QueryOptions queryOptions = new QueryOptions();

		queryOptions.setIndex(new NYCTLCDimensionalityTypeProvider().createPrimaryIndex());
//		final Query query = new SpatialQuery(new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"));
		final Query query = new NYCTLCQuery(
				0,
				(int)TimeUnit.DAYS.toSeconds(1),
				new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"),
				new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"));

		queryOptions.setAggregation(new CountAggregation<>(),
				new NYCTLCDataAdapter(NYCTLCUtils.createPointDataType()));

		if (queryOptions.getAggregation() != null) {
			final CloseableIterator<CountResult> results = plugin.createDataStore().query(
					queryOptions,
					query);

			while (results.hasNext()) {
				final CountResult stats = results.next();

				System.out.println(stats.getCount());
			}
			results.close();
		}
	}
}