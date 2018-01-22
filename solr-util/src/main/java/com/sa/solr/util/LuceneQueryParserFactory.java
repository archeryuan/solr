package com.sa.solr.util;

import java.util.Set;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * Factory class to build query parser
 * 
 * @author Kelvin Wong
 * 
 */
public class LuceneQueryParserFactory {

	private static class Holder {
		private static final LuceneQueryParserFactory instance = new LuceneQueryParserFactory();
	}

	public static LuceneQueryParserFactory getInstance() {
		return Holder.instance;
	}

	private LuceneQueryParserFactory() {

	}

	public QueryParser buildDefaultQueryParser() {
		return new QueryParser(Version.LUCENE_41, null, new IKAnalyzer());
	}

	public QueryParser buildNumericFriendlyQueryParser(Set<String> numericFields) {
		return new NumericFriendlyQueryParser(numericFields, Version.LUCENE_41, null, new IKAnalyzer());
	}
}
