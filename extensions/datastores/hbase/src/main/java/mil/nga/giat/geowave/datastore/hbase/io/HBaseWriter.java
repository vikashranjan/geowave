package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.Writer;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Functionality similar to <code> BatchWriterWrapper </code>
 *
 * This class directly writes to the HBase table instead of using any existing Writer API provided by HBase.
 *
 */
public class HBaseWriter implements
		Writer<RowMutations>
{
	private final static Logger LOGGER = Logger.getLogger(HBaseWriter.class);
	private final TableName tableName;
	private final Admin admin;

	private final HashMap<String, Boolean> cfMap;
	private HTableDescriptor tableDescriptor = null;
	private final BufferedMutator mutator;

	private final boolean schemaUpdateEnabled;

	static {
		LOGGER.setLevel(Level.DEBUG);
	}

	public HBaseWriter(
			final Admin admin,
			final String tableName,
			final BufferedMutator mutator ) {
		this.admin = admin;
		this.tableName = TableName.valueOf(tableName);
		this.mutator = mutator;

		cfMap = new HashMap<String, Boolean>();

		schemaUpdateEnabled = admin.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);
		
		LOGGER.debug("Schema Update Enabled = " + schemaUpdateEnabled);
		
		String check = admin.getConfiguration().get("hbase.online.schema.update.enable");
		if (check == null) {
			LOGGER.warn("'hbase.online.schema.update.enable' property should be true for best performance");
		}
	}

	@Override
	public void write(
			final RowMutations rowMutation ) {
		try {
			final long hack = System.currentTimeMillis();
			mutator.mutate(rowMutation.getMutations());
			DataStoreUtils.addToAccumulator(
					"hbaseWrite",
					System.currentTimeMillis() - hack);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to write mutation.",
					e);
		}
	}

	@Override
	public void write(
			final Iterable<RowMutations> mutations ) {
		for (final RowMutations rowMutation : mutations) {
			write(rowMutation);
		}
	}

	@Override
	public void close() {
		try {
			mutator.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	public void write(
			final Iterable<RowMutations> iterable,
			final String columnFamily )
			throws IOException {
		if (!columnFamilyExists(columnFamily)) {
			addColumnFamilyToTable(
					tableName,
					columnFamily);
		}

		for (final RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void write(
			final List<Put> puts,
			final String columnFamily )
			throws IOException {
		if (!columnFamilyExists(columnFamily)) {
			addColumnFamilyToTable(
					tableName,
					columnFamily);
		}

		mutator.mutate(puts);
	}

	public void write(
			final RowMutations mutation,
			final String columnFamily ) {
		try {
			synchronized (cfMap) {
				if (!columnFamilyExists(columnFamily)) {
					addColumnFamilyToTable(
							tableName,
							columnFamily);
				}
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to add column family " + columnFamily,
					e);
		}

		write(mutation);
	}

	private boolean columnFamilyExists(
			final String columnFamily ) {
		Boolean found = false;

		try {
			found = cfMap.get(columnFamily);

			if (found == null) {
				found = Boolean.FALSE;
			}

			if (!found) {
				synchronized (BasicHBaseOperations.ADMIN_MUTEX) {
					if (!schemaUpdateEnabled && !admin.isTableEnabled(tableName)) {
						admin.enableTable(tableName);
					}

					// update the table descriptor
					tableDescriptor = admin.getTableDescriptor(tableName);

					found = tableDescriptor.hasFamily(columnFamily.getBytes(StringUtils.UTF8_CHAR_SET));
				}

				cfMap.put(
						columnFamily,
						found);
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to check existence of column family " + columnFamily,
					e);
		}

		return found;
	}

	private void addColumnFamilyToTable(
			final TableName tableName,
			final String columnFamilyName )
			throws IOException {
		LOGGER.debug("Creating column family: " + columnFamilyName);

		final HColumnDescriptor cfDescriptor = new HColumnDescriptor(
				columnFamilyName);

		synchronized (BasicHBaseOperations.ADMIN_MUTEX) {
			if (!schemaUpdateEnabled && !admin.isTableDisabled(tableName)) {
				admin.disableTable(tableName);
			}
			
			admin.addColumn(
					tableName,
					cfDescriptor);
			cfMap.put(
					columnFamilyName,
					Boolean.TRUE);

			// Enable table once done
			if (!schemaUpdateEnabled) {
				admin.enableTable(tableName);
			}
		}
	}

	public void delete(
			final Iterable<RowMutations> iterable )
			throws IOException {
		for (final RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void delete(
			final Delete delete )
			throws IOException {
		mutator.mutate(delete);
	}

	public void delete(
			final List<Delete> deletes )
			throws IOException {
		mutator.mutate(deletes);
	}

	@Override
	public void flush() {
		try {
			mutator.flush();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to flush BufferedMutator",
					e);
		}
	}

}
