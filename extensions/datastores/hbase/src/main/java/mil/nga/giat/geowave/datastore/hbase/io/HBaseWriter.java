package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.Writer;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * Functionality similar to <code> BatchWriterWrapper </code>
 *
 * This class directly writes to the HBase table instead of using any existing
 * Writer API provided by HBase.
 *
 */
public class HBaseWriter implements
		Writer<RowMutations>
{

	private final static Logger LOGGER = Logger.getLogger(HBaseWriter.class);
	private final Table table;
	private final Admin admin;

	public HBaseWriter(
			final Admin admin,
			final Table table ) {
		this.admin = admin;
		this.table = table;
		
		LOGGER.setLevel(Level.DEBUG);
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
		addColumnFamilyToTable(
				table.getName(),
				columnFamily);
		for (final RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void write(
			final RowMutations mutation,
			final String columnFamily ) {
		try {
			addColumnFamilyToTable(
					table.getName(),
					columnFamily);
			write(mutation);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to add column family " + columnFamily,
					e);
		}
	}

	private void addColumnFamilyToTable(
			final TableName tableName,
			final String columnFamilyName )
			throws IOException {
		final HColumnDescriptor cfDescriptor = new HColumnDescriptor(
				columnFamilyName);
		
		// Instead of an object lock, we need to switch to async disable/enable w/ wait loops.
//		synchronized (BasicHBaseOperations.ADMIN_MUTEX) {
			if (admin.tableExists(tableName)) {
				if (!admin.isTableEnabled(tableName)) {
					admin.enableTableAsync(tableName);
					while (!admin.isTableEnabled(tableName)) {
						try {
							Thread.sleep(250);
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				
				final HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
				
				if (!tableDescriptor.hasFamily(columnFamilyName.getBytes())) {
					LOGGER.debug("Table " + tableName.getNameAsString() + " adding column family " + columnFamilyName);
					
					// Disable the table before adding the cf
					if (admin.isTableEnabled(tableName)) {
						admin.disableTableAsync(tableName);
						while (!admin.isTableDisabled(tableName)) {
							try {
								Thread.sleep(250);
							}
							catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
					
					// Add the column family
					admin.addColumn(
							tableName,
							cfDescriptor);
					
					// Enable table once done
					admin.enableTableAsync(tableName);
					while (!admin.isTableEnabled(tableName)) {
						try {
							Thread.sleep(250);
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				else {
					LOGGER.debug("Table " + tableName.getNameAsString() + " already has column family " + columnFamilyName);
				}
			}
			else {
				LOGGER.warn("Table " + tableName.getNameAsString()
						+ " doesn't exist! Unable to add column family " + columnFamilyName);
			}
//		}
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
