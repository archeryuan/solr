package com.sa.solr.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class SolrDateUtil {

	private static DateFormat solrDateFormat;

	static {
		solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		solrDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public static String getSolrDateStr(Date date) throws ParseException {
		String result = solrDateFormat.format(date);
		return result;
	}

	public static Date getSolrDate(String solrDateStr) throws ParseException {
		return solrDateFormat.parse(solrDateStr);
	}
}
