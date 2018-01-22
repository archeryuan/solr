package com.sa.solr.domain;

import java.util.Date;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class Feed {
	private String title;
	private String content;
	private String url;
	private Date pDate;
	private Date cDate;
	private String photo;
	private Long views;
	private Long likes;
	private Long replies;
	private Long comments;
	private Long shares;
	private Long dislikes;
	private String sourceType;
	private String sourceDomain;
	private Float sScore;
	private String author;
	private String region;
	private String feedId;
	private String uId;
	private String postId;
	private Set<String> discoveredKeywords;
	private Long angryCnt;
	private Long hahaCnt;
	private Long loveCnt;
	private Long sadCnt;
	private Long thankfulCnt;
	private Long wowCnt;
	
	private Long pttTuiCnt;
	private Long pttXuCnt;
	private Long pttJianCnt;
	private String pttLocation;
	
	private Set<String> matchedKeywords;
	private String language;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Date getpDate() {
		return pDate;
	}

	public void setpDate(Date pDate) {
		this.pDate = pDate;
	}

	public String getPhoto() {
		return photo;
	}

	public void setPhoto(String photo) {
		this.photo = photo;
	}

	public Long getViews() {
		return views;
	}

	public void setViews(Long views) {
		this.views = views;
	}

	public Long getLikes() {
		return likes;
	}

	public void setLikes(Long likes) {
		this.likes = likes;
	}

	public Long getReplies() {
		return replies;
	}

	public void setReplies(Long replies) {
		this.replies = replies;
	}

	public Long getComments() {
		return comments;
	}

	public void setComments(Long comments) {
		this.comments = comments;
	}

	public Long getShares() {
		return shares;
	}

	public void setShares(Long shares) {
		this.shares = shares;
	}

	public Long getDislikes() {
		return dislikes;
	}

	public void setDislikes(Long dislikes) {
		this.dislikes = dislikes;
	}

	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	public String getSourceDomain() {
		return sourceDomain;
	}

	public void setSourceDomain(String sourceDomain) {
		this.sourceDomain = sourceDomain;
	}

	/**
	 * @return the sScore
	 */
	public Float getsScore() {
		return sScore;
	}

	/**
	 * @param sScore
	 *            the sScore to set
	 */
	public void setsScore(Float sScore) {
		this.sScore = sScore;
	}

	/**
	 * @return the author
	 */
	public String getAuthor() {
		return author;
	}

	/**
	 * @param author
	 *            the author to set
	 */
	public void setAuthor(String author) {
		this.author = author;
	}

	/**
	 * @return the region
	 */
	public String getRegion() {
		return region;
	}

	/**
	 * @param region
	 *            the region to set
	 */
	public void setRegion(String region) {
		this.region = region;
	}

	public String getuId() {
		return uId;
	}

	public void setuId(String uId) {
		this.uId = uId;
	}

	public String getFeedId() {
		return feedId;
	}

	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}

	public String getPostId() {
		return postId;
	}

	public void setPostId(String postId) {
		this.postId = postId;
	}

	public Set<String> getDiscoveredKeywords() {
		return discoveredKeywords;
	}

	public void setDiscoveredKeywords(Set<String> discoveredKeywords) {
		this.discoveredKeywords = discoveredKeywords;
	}

	/**
	 * @return the angryCnt
	 */
	public Long getAngryCnt() {
		return angryCnt;
	}

	/**
	 * @return the hahaCnt
	 */
	public Long getHahaCnt() {
		return hahaCnt;
	}

	/**
	 * @return the loveCnt
	 */
	public Long getLoveCnt() {
		return loveCnt;
	}

	/**
	 * @return the sadCnt
	 */
	public Long getSadCnt() {
		return sadCnt;
	}

	/**
	 * @return the thankfulCnt
	 */
	public Long getThankfulCnt() {
		return thankfulCnt;
	}

	/**
	 * @return the wowCnt
	 */
	public Long getWowCnt() {
		return wowCnt;
	}

	/**
	 * @param angryCnt
	 *            the angryCnt to set
	 */
	public void setAngryCnt(Long angryCnt) {
		this.angryCnt = angryCnt;
	}

	/**
	 * @param hahaCnt
	 *            the hahaCnt to set
	 */
	public void setHahaCnt(Long hahaCnt) {
		this.hahaCnt = hahaCnt;
	}

	/**
	 * @param loveCnt
	 *            the loveCnt to set
	 */
	public void setLoveCnt(Long loveCnt) {
		this.loveCnt = loveCnt;
	}

	/**
	 * @param sadCnt
	 *            the sadCnt to set
	 */
	public void setSadCnt(Long sadCnt) {
		this.sadCnt = sadCnt;
	}

	/**
	 * @param thankfulCnt
	 *            the thankfulCnt to set
	 */
	public void setThankfulCnt(Long thankfulCnt) {
		this.thankfulCnt = thankfulCnt;
	}

	/**
	 * @param wowCnt
	 *            the wowCnt to set
	 */
	public void setWowCnt(Long wowCnt) {
		this.wowCnt = wowCnt;
	}

	public Set<String> getMatchedKeywords() {
		return matchedKeywords;
	}

	public void setMatchedKeywords(Set<String> matchedKeywords) {
		this.matchedKeywords = matchedKeywords;
	}

	public Date getcDate() {
		return cDate;
	}

	public void setcDate(Date cDate) {
		this.cDate = cDate;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Long getPttTuiCnt() {
		return pttTuiCnt;
	}

	public void setPttTuiCnt(Long pttTuiCnt) {
		this.pttTuiCnt = pttTuiCnt;
	}

	public Long getPttXuCnt() {
		return pttXuCnt;
	}

	public void setPttXuCnt(Long pttXuCnt) {
		this.pttXuCnt = pttXuCnt;
	}

	public Long getPttJianCnt() {
		return pttJianCnt;
	}

	public void setPttJianCnt(Long pttJianCnt) {
		this.pttJianCnt = pttJianCnt;
	}

	public String getPttLocation() {
		return pttLocation;
	}

	public void setPttLocation(String pttLocation) {
		this.pttLocation = pttLocation;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
