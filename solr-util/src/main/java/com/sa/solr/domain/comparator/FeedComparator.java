/**
 * 
 */
package com.sa.solr.domain.comparator;

import java.util.Comparator;
import java.util.Date;

import org.apache.solr.client.solrj.SolrQuery.ORDER;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.solr.domain.Feed;

/**
 * @author lewis
 *
 */
public class FeedComparator implements Comparator<Feed> {

	private SolrFieldDefinition solrField;
	private ORDER order;

	public FeedComparator(SolrFieldDefinition solrField, ORDER order) {
		super();
		this.solrField = (null == solrField) ? SolrFieldDefinition.PUBLISH_DATE : solrField;
		this.order = (null == order) ? ORDER.desc : order;
	}

	@Override
	public int compare(Feed o1, Feed o2) {
		if (null != o1 && null != o2) {
			switch (solrField) {
			case PUBLISH_DATE:
				Date d1 = o1.getpDate();
				Date d2 = o2.getpDate();
				if (null == d1 && null != d2) {
					return ORDER.desc == order ? 1 : -1;
				} else if (null != d1 && null == d2) {
					return ORDER.desc == order ? -1 : 1;
				} else if (null != d1 && null != d2) {
					return ORDER.desc == order ? d2.compareTo(d1) : -d2.compareTo(d1);
				}
				break;
			case LIKE_COUNT:
				Long e1 = 0l;
				if (o1.getLikes() != null) {
					e1 = e1 + o1.getLikes();
				}
				if (o1.getComments() != null) {
					e1 = e1 + o1.getComments();
				}
				if (o1.getShares() != null) {
					e1 = e1 + o1.getShares();
				}
				Long e2 = 0l;
				if (o2.getLikes() != null) {
					e2 = e2 + o2.getLikes();
				}
				if (o2.getComments() != null) {
					e2 = e2 + o2.getComments();
				}
				if (o2.getShares() != null) {
					e2 = e2 + o2.getShares();
				}
				if (null != e1 && null != e2) {
					return ORDER.desc == order ? e2.compareTo(e1) : -e2.compareTo(e1);
				}
				break;
			default:
				break;
			}

		}

		return 0;
	}

}
