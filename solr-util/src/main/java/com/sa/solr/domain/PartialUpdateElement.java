package com.sa.solr.domain;

import java.util.Collection;

import com.sa.solr.definition.PartialUpdate;
import com.sa.solr.definition.SolrField;

public class PartialUpdateElement {

	private SolrField solrField;
	private String str_value;
	private Long long_value;
	private Double double_value;
	private Boolean bool_value;
	private Collection coll_value;
	private Object value;
	private PartialUpdate oper;

	public SolrField getSolrField() {
		return solrField;
	}

	public void setSolrField(SolrField solrField) {
		this.solrField = solrField;
	}

	public String getStr_value() {
		return str_value;
	}

	public void setStr_value(String str_value) {
		this.str_value = str_value;
	}

	public Long getLong_value() {
		return long_value;
	}

	public void setLong_value(long long_value) {
		this.long_value = long_value;
	}

	public Double getDouble_value() {
		return double_value;
	}

	public void setDouble_value(double double_value) {
		this.double_value = double_value;
	}

	public Boolean isBool_value() {
		return bool_value;
	}

	public void setBool_value(boolean bool_value) {
		this.bool_value = bool_value;
	}

	public Collection getColl_value() {
		return coll_value;
	}

	public void setColl_value(Collection coll_value) {
		this.coll_value = coll_value;
	}

	public PartialUpdate getOper() {
		return oper;
	}

	public void setOper(PartialUpdate oper) {
		this.oper = oper;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public PartialUpdateElement(SolrField solrField, String str_value,
			PartialUpdate oper) {
		this.solrField = solrField;
		this.str_value = str_value;
		this.oper = oper;
	}

	public PartialUpdateElement(SolrField solrField, long long_value,
			PartialUpdate oper) {
		this.solrField = solrField;
		this.long_value = long_value;
		this.oper = oper;
	}

	public PartialUpdateElement(SolrField solrField, double double_value,
			PartialUpdate oper) {
		this.solrField = solrField;
		this.double_value = double_value;
		this.oper = oper;
	}

	public PartialUpdateElement(SolrField solrField, boolean bool_value,
			PartialUpdate oper) {
		this.solrField = solrField;
		this.bool_value = bool_value;
		this.oper = oper;
	}

	public PartialUpdateElement(SolrField solrField, Collection coll_value,
			PartialUpdate oper) {
		this.solrField = solrField;
		this.coll_value = coll_value;
		this.oper = oper;
	}

	public PartialUpdateElement(SolrField solrField, Object value,
			PartialUpdate oper) {
		this.solrField = solrField;
		this.value = value;
		this.oper = oper;
	}

}
