package mil.nga.giat.geowave.datastore.accumulo.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Range;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.Persistable;

public class ByteArrayRangesPersistable implements Persistable
{
	private List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
	public ByteArrayRangesPersistable(){}
	public ByteArrayRangesPersistable(
			List<ByteArrayRange> ranges ) {
		super();
		this.ranges = ranges;
	}

	public List<ByteArrayRange> getRanges() {
		return ranges;
	}
	@Override
	public byte[] toBinary() {
		int capacity = 4;
		List<byte[]> output = new ArrayList<byte[]>();
		if (ranges != null){
		for (ByteArrayRange r : ranges){
			byte[] start = r.getStart().getBytes();
			output.add(start);
			byte[] end = r.getEnd().getBytes();
			output.add(end);
			capacity+=8;
			capacity+=start.length;
			capacity+=end.length;
		}
		}
		ByteBuffer buf =ByteBuffer.allocate(capacity);
		buf.putInt(output.size()/2);
		for (byte[] o : output){
			buf.putInt(o.length);
			buf.put(o);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		int numRanges = buf.getInt();
		ranges = new ArrayList<ByteArrayRange>();
		for (int i =0; i < numRanges; i++){
			int size = buf.getInt();
			byte[] start = new byte[size];
			buf.get(start);
			size = buf.getInt();
			byte[] end = new byte[size];
			buf.get(end);
			ranges.add(new ByteArrayRange(new ByteArrayId(start), new ByteArrayId(end)));
		}
	}
	

}
