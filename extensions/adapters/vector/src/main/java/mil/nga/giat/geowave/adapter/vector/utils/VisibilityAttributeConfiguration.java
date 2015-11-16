package mil.nga.giat.geowave.adapter.vector.utils;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;

import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * Determine which attribute determines the visibility of the feature
 * 
 */
public class VisibilityAttributeConfiguration implements
		SimpleFeatureUserDataConfiguration
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String visibilityAttributeName;

	public String getVisibilityAttributeName() {
		return visibilityAttributeName;
	}

	public void setVisibilityAttributeName(
			String visibilityAttributeName ) {
		this.visibilityAttributeName = visibilityAttributeName;
	}

	public VisibilityAttributeConfiguration() {}

	public VisibilityAttributeConfiguration(
			final SimpleFeatureType type ) {
		this.configureFromType(type);
	}

	@Override
	public void updateType(
			SimpleFeatureType type ) {
		if (visibilityAttributeName != null && type.getDescriptor(visibilityAttributeName) != null) {
			type.getUserData().put(
					FeatureDataAdapter.VISIBILITY_ATT_NAME,
					visibilityAttributeName);
			type.getDescriptor(
					visibilityAttributeName).getUserData().put(
					"visibility",
					Boolean.TRUE);
		}

	}

	@Override
	public void configureFromType(
			SimpleFeatureType type ) {
		final Object obj = type.getUserData().get(
				FeatureDataAdapter.VISIBILITY_ATT_NAME);
		visibilityAttributeName = (obj == null) ? "GEOWAVE_VISIBILITY" : obj.toString();
		// backward compat
		for (final AttributeDescriptor attrDesc : type.getAttributeDescriptors()) {
			final Boolean isVisibility = (Boolean) attrDesc.getUserData().get(
					"visibility");
			if ((isVisibility != null) && isVisibility.booleanValue()) {
				visibilityAttributeName = attrDesc.getLocalName();
			}
		}

	}

}
