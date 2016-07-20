package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.Writer;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

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
	private final Table table;
	private final Admin admin;

	private final HashMap<String, Boolean> cfMap;
	private Boolean tableExists = null;
	private HTableDescriptor tableDescriptor = null;

	public HBaseWriter(
			final Admin admin,
			final Table table ) {
		this.admin = admin;
		this.table = table;

		this.cfMap = new HashMap<String, Boolean>();
	}

	@Override
	public void write(
			final RowMutations rowMutation ) {
		try {
			table.mutateRow(rowMutation);
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
	public void close() {}

	public void write(
			final Iterable<RowMutations> iterable,
			final String columnFamily )
			throws IOException {
		if (!columnFamilyExists(columnFamily)) {
			addColumnFamilyToTable(
					table.getName(),
					columnFamily);
		}

		for (final RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void write(
			final RowMutations mutation,
			final String columnFamily ) {
		try {
			if (!columnFamilyExists(columnFamily)) {
				addColumnFamilyToTable(
						table.getName(),
						columnFamily);
			}

			write(mutation);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to add column family " + columnFamily,
					e);
		}
	}

	private boolean columnFamilyExists(
			final String columnFamily ) {
		Boolean found = false;

		try {
			if (tableExists == null) {
				tableExists = admin.tableExists(table.getName());
			}

			if (!tableExists) {
				return false;
			}

			found = cfMap.get(columnFamily);

			if (found == null) {
				found = Boolean.FALSE;
			}

			if (!found) {
				// need to enable table here?
				if (tableDescriptor == null) {
					tableDescriptor = admin.getTableDescriptor(table.getName());
				}

				found = tableDescriptor.hasFamily(columnFamily.getBytes());

				cfMap.put(
						columnFamily,
						found);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		return found;
	}

	private void addColumnFamilyToTable(
			final TableName tableName,
			final String columnFamilyName )
			throws IOException {
		final HColumnDescriptor cfDescriptor = new HColumnDescriptor(
				columnFamilyName);

		synchronized (BasicHBaseOperations.ADMIN_MUTEX) {
			if (tableExists) {
				if (!admin.isTableDisabled(tableName)) {
					admin.disableTable(tableName);
				}

				admin.addColumn(
						tableName,
						cfDescriptor);

				// Enable table once done
				admin.enableTable(tableName);
			}
			else {
				LOGGER.warn("Table " + tableName.getNameAsString() + " doesn't exist! Unable to add column family " + columnFamilyName);
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
		table.delete(delete);
	}

	public void delete(
			final List<Delete> deletes )
			throws IOException {
		table.delete(deletes);
	}

	@Override
	public void flush() {}

}
