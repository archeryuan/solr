package com.sa.solr.domain.comparator;

import java.util.Comparator;
import java.util.Date;

import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.common.SolrDocument;

import com.sa.common.definition.SolrFieldDefinition;

public class SolrDocumentComparator implements Comparator<SolrDocument>{
	
	private SolrFieldDefinition solrField;
	
	private ORDER order;

	public SolrDocumentComparator(SolrFieldDefinition solrField, ORDER order) {
		super();
		this.solrField = (null == solrField) ? SolrFieldDefinition.PUBLISH_DATE : solrField;
		this.order = (null == order) ? ORDER.desc : order;
	}
	
	@Override
	public int compare(SolrDocument o1, SolrDocument o2) {
		if (null != o1 && null != o2) {
			switch (solrField) {
			case PUBLISH_DATE:
				Date d1 = (Date) o1.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				Date d2 = (Date)o2.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if (null == d1 && null != d2) {
					return ORDER.desc == order ? 1 : -1;
				} else if (null != d1 && null == d2) {
					return ORDER.desc == order ? -1 : 1;
				} else if (null != d1 && null != d2) {
					return ORDER.desc == order ? d2.compareTo(d1) : -d2.compareTo(d1);
				}
				break;
			default:
				break;
			}

		}
		return 0;
	}

}
