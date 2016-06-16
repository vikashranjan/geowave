package mil.nga.giat.geowave.core.store;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.apache.log4j.Logger;

public class GeoWaveUrlStreamHandler extends
		URLStreamHandler
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveUrlStreamHandler.class);
	public static final String GW_PROTOCOL = "geowave"; // GeoWave Custom Protocol

	@Override
	protected URLConnection openConnection(
			URL url )
			throws IOException {
		return new URLConnection(
				url) {
			public void connect()
					throws IOException {
				LOGGER.info(GW_PROTOCOL + " custom protocol registered.");
			}
		};
	}

}
