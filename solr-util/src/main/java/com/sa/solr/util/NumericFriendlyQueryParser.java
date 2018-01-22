package com.sa.solr.util;

import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

/**
 * A parser overriding default QueryParser to provide functionality on range search numeric fields
 * 
 * @author Kelvin Wong
 * 
 */
public class NumericFriendlyQueryParser extends QueryParser {

	private Set<String> numericFields;

	public NumericFriendlyQueryParser(Set<String> numericFields, Version matchVersion, String f, Analyzer a) {
		super(matchVersion, f, a);
		this.numericFields = numericFields;
	}

	@Override
	protected org.apache.lucene.search.Query getRangeQuery(String field, String startNum, String endNum, boolean startInclusive,
			boolean endInclusive) throws ParseException {

		if (!CollectionUtils.isEmpty(numericFields) && numericFields.contains(field)) {
			try {
				BytesRef num1Bytes = new BytesRef();
				NumericUtils.intToPrefixCoded(Integer.parseInt(startNum), 0, num1Bytes);
				BytesRef num2Bytes = new BytesRef();
				NumericUtils.intToPrefixCoded(Integer.parseInt(endNum), 0, num2Bytes);
				return new TermRangeQuery(field, num1Bytes, num2Bytes, startInclusive, endInclusive);
			} catch (NumberFormatException e) {
				throw new ParseException(e.getMessage());
			}
		}

		return super.getRangeQuery(field, startNum, endNum, startInclusive, endInclusive);
	}
}
