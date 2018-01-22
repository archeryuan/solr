package com.sa.solr.definition;

public enum SolrField {

	id("id"),

	pDate("pDate"),

	uName("uName"),

	siteId("siteId"),

	SNSsiteId("SNSsiteId"),

	aURL("aURL"),

	sType("sType"),

	title("title"),

	content("content"),

	region("region"),

	cDate("cDate"),

	domain("domain"),

	sMent("sMent"),

	cat("cat"),

	type("type"),

	pIds("pIds"),

	imgURL("imgURL"),

	videoURL("videoURL"),

	email("email"),

	uid("uid"),

	emotion("emotion"),

	link("link"),

	pDateStr("pDateStr"),

	pHour("pHour"),

	comCount("comCount"),

	retCount("retCount"),

	isRub("isRub"),

	isVIP("isVIP"),

	emgTopicIds("emgTopicIDs"),

	hotTopicIds("hotTopicIds"),

	profileClusterIds("pfClusterIds"),

	rID("rID"),

	rUserId("rUserId"),

	tweetType("tweetType"),

	birthday("birthday"),

	occupation("occupation"),

	education("education"),

	gender("gender"),

	regionPId("regionPId"),

	tweetId("tweetId"),

	catTags("catTags"),
	
	bigImgURL("bigImgURL"),

	sentScore("sScore"),

	countryCode("countryCode"),

	clickCount("clickCount"),

	// Infocast Only
	reportScore("reportScore"),
	
	app("app");

	private String value;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	private SolrField(String value) {
		this.value = value;
	}

}
