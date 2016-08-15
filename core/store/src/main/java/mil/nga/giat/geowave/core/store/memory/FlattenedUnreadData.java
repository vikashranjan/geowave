package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

public interface FlattenedUnreadData
{
	public List<FlattenedFieldInfo> finishRead();
}
