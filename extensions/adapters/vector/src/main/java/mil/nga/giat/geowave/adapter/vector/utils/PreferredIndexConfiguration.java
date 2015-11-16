package mil.nga.giat.geowave.adapter.vector.utils;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;

import org.opengis.feature.simple.SimpleFeatureType;

/**
 *  Preferred index name
 *
 */
public class PreferredIndexConfiguration implements
		SimpleFeatureUserDataConfiguration
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String preferredIndexName = null;

	public String getPreferredIndexName() {
		return preferredIndexName;
	}

	public void setPreferredIndexName(
			String preferredIndexName ) {
		this.preferredIndexName = preferredIndexName;
	}
	
	public PreferredIndexConfiguration() {		
	}
	
	public PreferredIndexConfiguration(
			final SimpleFeatureType type ) {
		this.configureFromType(type);
	}

	@Override
	public void updateType(
			SimpleFeatureType type ) {
		type.getUserData().put(
				FeatureDataAdapter.PREFERRED_INDEX_ATT_NAME,
				preferredIndexName);

	}

	@Override
	public void configureFromType(
			SimpleFeatureType type ) {
		final Object obj = type.getUserData().get(
				FeatureDataAdapter.PREFERRED_INDEX_ATT_NAME);
		preferredIndexName = (obj == null) ? null : obj.toString();

	}

}
