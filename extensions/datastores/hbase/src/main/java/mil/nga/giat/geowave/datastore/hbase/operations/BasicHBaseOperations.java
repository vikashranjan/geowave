package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.log4j.Logger;

public class BasicHBaseOperations implements
		DataStoreOperations
{
	private final static Logger LOGGER = Logger.getLogger(BasicHBaseOperations.class);
	public static final Object ADMIN_MUTEX = new Object();

	private final Connection conn;
	private final Admin admin;

	private final String tableNamespace;
	private final HashMap<String, List<String>> tableCfMap;

	public BasicHBaseOperations(
			final String zookeeperInstances,
			final String geowaveNamespace )
			throws IOException {
		conn = ConnectionPool.getInstance().getConnection(
				zookeeperInstances);

		admin = conn.getAdmin();

		tableNamespace = geowaveNamespace;

		tableCfMap = new HashMap<String, List<String>>();
	}

	public static BasicHBaseOperations createOperations(
			final HBaseRequiredOptions options )
			throws IOException {
		return new BasicHBaseOperations(
				options.getZookeeper(),
				options.getGeowaveNamespace());
	}

	private TableName getTableName(
			final String tableName ) {
		return TableName.valueOf(tableName);
	}

	public HBaseWriter createWriter(
			final String sTableName,
			final String columnFamily,
			final boolean createTable )
			throws IOException {
		final TableName tName = getTableName(getQualifiedTableName(sTableName));
		Table table = getTable(
				createTable,
				columnFamily,
				tName);

		return new HBaseWriter(
				admin,
				table);
	}

	/*
	 * private Table getTable( final boolean create, TableName name ) throws IOException { return getTable( create,
	 * DEFAULT_COLUMN_FAMILY, name); }
	 */

	private Table getTable(
			final boolean create,
			final String columnFamily,
			final TableName tableName )
			throws IOException {
		if (create && !admin.isTableAvailable(tableName)) {
			synchronized (ADMIN_MUTEX) {
				final HTableDescriptor desc = new HTableDescriptor(
						tableName);
				desc.addFamily(new HColumnDescriptor(
						columnFamily));
				admin.createTable(desc);
			}
		}

		return conn.getTable(tableName);
	}

	public String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return HBaseUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

	@Override
	public void deleteAll()
			throws IOException {
		final TableName[] tableNamesArr = admin.listTableNames();
		for (final TableName tableName : tableNamesArr) {
			if ((tableNamespace == null) || tableName.getNameAsString().startsWith(
					tableNamespace)) {
				synchronized (ADMIN_MUTEX) {
					if (admin.isTableAvailable(tableName)) {
						admin.disableTable(tableName);
						admin.deleteTable(tableName);
					}
				}
			}
		}
	}

	@Override
	public boolean tableExists(
			final String tableName )
			throws IOException {
		final String qName = getQualifiedTableName(tableName);

		return admin.isTableAvailable(getTableName(qName));
	}

	public boolean columnFamilyExists(
			final String tableName,
			final String columnFamily )
			throws IOException {
		final String qName = getQualifiedTableName(tableName);

		List<String> cfList = tableCfMap.get(qName);

		if (cfList != null) {
			if (cfList.contains(columnFamily)) {
				return true;
			}
		}
		else {
			cfList = new ArrayList<String>();
		}

		final HTableDescriptor descriptor = admin.getTableDescriptor(getTableName(qName));

		if (descriptor != null) {
			if (descriptor.hasFamily(columnFamily.getBytes())) {
				cfList.add(columnFamily);
				
				tableCfMap.put(
						qName,
						cfList);
				
				return true;
			}
		}

		return false;
	}

	public ResultScanner getScannedResults(
			final Scan scanner,
			final String tableName,
			final String... authorizations )
			throws IOException {
		if (authorizations != null) {
			scanner.setAuthorizations(new Authorizations(
					authorizations));
		}
		return conn.getTable(
				getTableName(getQualifiedTableName(tableName))).getScanner(
				scanner);
	}

	public boolean deleteTable(
			final String tableName ) {
		final String qName = getQualifiedTableName(tableName);
		try {
			admin.deleteTable(getTableName(qName));
			return true;
		}
		catch (final IOException ex) {
			LOGGER.warn(
					"Unable to delete table '" + qName + "'",
					ex);
		}
		return false;

	}

	public RegionLocator getRegionLocator(
			final String tableName )
			throws IOException {
		return conn.getRegionLocator(getTableName(getQualifiedTableName(tableName)));
	}

	@Override
	public String getTableNameSpace() {
		return tableNamespace;
	}
}
