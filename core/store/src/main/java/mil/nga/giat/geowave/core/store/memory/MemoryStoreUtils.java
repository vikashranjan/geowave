package mil.nga.giat.geowave.core.store.memory;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class MemoryStoreUtils
{
	protected static IndexedAdapterPersistenceEncoding getEncoding(
			final CommonIndexModel model,
			final DataAdapter<?> adapter,
			final MemoryEntryRow row ) {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		for (final FieldInfo column : row.info.getFieldInfo()) {
			final FieldReader<? extends CommonIndexValue> reader = model.getReader(column.getDataValue().getId());
			if (reader == null) {
				final FieldReader extendedReader = adapter.getReader(column.getDataValue().getId());
				if (extendedReader != null) {
					extendedData.addValue(column.getDataValue());
				}
				else {
					unknownData.addValue(new PersistentValue<byte[]>(
							column.getDataValue().getId(),
							column.getWrittenValue()));
				}
			}
			else {
				commonData.addValue(column.getDataValue());
			}
		}
		return new IndexedAdapterPersistenceEncoding(
				new ByteArrayId(
						row.getTableRowId().getAdapterId()),
				new ByteArrayId(
						row.getTableRowId().getDataId()),
				new ByteArrayId(
						row.getTableRowId().getInsertionId()),
				row.getTableRowId().getNumberOfDuplicates(),
				commonData,
				unknownData,
				extendedData);
	}

	public static <T> List<MemoryEntryRow> entryToRows(
			final WritableDataAdapter<T> dataWriter,
			final PrimaryIndex index,
			final T entry,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				dataWriter,
				index,
				entry,
				customFieldVisibilityWriter);
		ingestCallback.entryIngested(
				ingestInfo,
				entry);
		return buildRows(
				dataWriter.getAdapterId().getBytes(),
				entry,
				ingestInfo);
	}

	private static <T> List<MemoryEntryRow> buildRows(
			final byte[] adapterId,
			final T entry,
			final DataStoreEntryInfo ingestInfo ) {
		final List<MemoryEntryRow> rows = new ArrayList<MemoryEntryRow>();
		for (final ByteArrayId rowId : ingestInfo.getRowIds()) {
			rows.add(new MemoryEntryRow(
					rowId,
					entry,
					ingestInfo));
		}
		return rows;
	}
}
