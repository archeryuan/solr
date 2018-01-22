/**
 * 
 */
package social.hunt.solr.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import social.hunt.common.definition.SourceType;

/**
 * @author lewis
 *
 */
public enum SolrCollection {
	// NEWS("news"),
	SOCIAL_MEDIA("social-media"),
	// DISCUSSION("discussion"),
	OTHERS("others");

	private final String value;

	/**
	 * @param value
	 */
	private SolrCollection(String value) {
		this.value = value;
	}

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	public static Collection<SolrCollection> getCollectionBySourceType(List<SourceType> sourceTypes) {
		Collection<SolrCollection> collections = new HashSet<SolrCollection>();
		for (SourceType sourceType : sourceTypes) {
			switch (sourceType) {
			case BLOG:
			case NEWS:
			case GOVERNMENT:
			case QA:
				collections.add(OTHERS);
				break;
			case FORUM:
				collections.add(OTHERS);
				break;
			case SNS:
				collections.add(SOCIAL_MEDIA);
				break;
			case OTHERS:
				collections.add(OTHERS);
				break;
			}
		}
		return collections;
	}

	/**
	 * Get a comma separated string of collections for Solr query.
	 * 
	 * @param sourceTypes
	 * @return
	 */
	public static String getCollectionStrBySourceType(List<SourceType> sourceTypes) {
		Collection<SolrCollection> collections = getCollectionBySourceType(sourceTypes);
		Collection<String> str = new ArrayList<String>();
		for (SolrCollection coll : collections) {
			str.add(coll.getValue());
		}
		return StringUtils.join(str, ",");
	}

	public static String getCollectionString(SolrCollection... collections) {
		List<String> values = new ArrayList<String>();
		for (SolrCollection coll : collections) {
			values.add(coll.getValue());
		}
		return StringUtils.join(values, ",");
	}

	/**
	 * Get a comma separated string of all collections for Solr query.
	 * 
	 * @return
	 */
	public static String getAllCollectionString() {
		List<String> values = new ArrayList<String>();
		for (SolrCollection coll : values()) {
			values.add(coll.getValue());
		}
		return StringUtils.join(values, ",");
	}

	public static String[] getAllCollectionArray() {
		List<String> values = new ArrayList<String>();
		for (SolrCollection coll : values()) {
			values.add(coll.getValue());
		}
		return values.toArray(new String[] {});
	}
}
