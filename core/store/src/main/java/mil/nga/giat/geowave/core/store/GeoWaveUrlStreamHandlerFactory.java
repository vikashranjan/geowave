package mil.nga.giat.geowave.core.store;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GeoWaveUrlStreamHandlerFactory implements
		URLStreamHandlerFactory
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveUrlStreamHandlerFactory.class);

	private java.net.URLStreamHandler geowaveHandler;
	private URLStreamHandlerFactory defaultFactory;
	private static GeoWaveUrlStreamHandlerFactory singleton;
	
//	static {
//		initialize();
//	}

	static public GeoWaveUrlStreamHandlerFactory getInstance() {
		if (singleton == null) {
			LOGGER.warn("GeoWaveUrlStreamHandlerFactory initialized prematurely.");
			initialize();
		}
		
		return singleton;
	}

	private static void initialize() {
		LOGGER.setLevel(Level.DEBUG);
		if (singleton != null) {
			LOGGER.warn("GeoWaveUrlStreamHandlerFactory already initialized!");
			return;
		}
		
		try {
			LOGGER.info("Initializing GeoWaveUrlStreamHandlerFactory...");
			singleton = new GeoWaveUrlStreamHandlerFactory();
			URL.setURLStreamHandlerFactory(singleton);
			LOGGER.info("...GeoWaveUrlStreamHandlerFactory OK");
		}
		catch (final Error factoryError) {
			String type = "";
			Field f = null;
			try {
				f = URL.class.getDeclaredField("factory");
			}
			catch (final NoSuchFieldException e) {
				LOGGER.error(
						"URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
						e);
				throw (factoryError);
			}
			f.setAccessible(true);
			Object o;
			try {
				o = f.get(null);
			}
			catch (final IllegalAccessException e) {
				LOGGER.error(
						"URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
						e);
				throw (factoryError);
			}
			if (o instanceof GeoWaveUrlStreamHandlerFactory) {
				LOGGER.info("setURLStreamHandlerFactory already set on this JVM to GeoWaveUrlStreamHandlerFactory.  Nothing to do");
				return;
			}
			else {
				type = o.getClass().getCanonicalName();
			}
			LOGGER.error("URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to: " + type);
			throw (factoryError);
		}
	}

	public GeoWaveUrlStreamHandlerFactory() {
		this.geowaveHandler = new GeoWaveUrlStreamHandler();
	}

	@Override
	public URLStreamHandler createURLStreamHandler(
			String protocol ) {
		LOGGER.info("Calling GeoWaveUrlStreamHandlerFactory with protocol = " + protocol);
		
		if (protocol.equals(GeoWaveUrlStreamHandler.GW_PROTOCOL)) {
			LOGGER.info("Found GeoWave Url Handler");
			return this.geowaveHandler;
		}

		if (defaultFactory != null) {
			LOGGER.info("Found Default Url Handler");
			return defaultFactory.createURLStreamHandler(protocol);
		}

		LOGGER.error("No Url Handler found for protocol = " + protocol);
		
		return null;
	}

	public void setDefaultURLStreamHandlerFactory(
			URLStreamHandlerFactory fac ) {
		defaultFactory = fac;
	}
}
