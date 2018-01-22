/**
 * 
 */
package social.hunt.solr.util;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.common.definition.Region;
import social.hunt.common.definition.Sns;
import social.hunt.common.definition.SourceType;
import social.hunt.language.util.ChineseTranslator;
import social.hunt.solr.connection.SolrClientManager;
import social.hunt.solr.definition.SolrCollection;

import social.hunt.common.definition.Language;
import com.google.common.collect.Sets;
import com.hankcs.hanlp.HanLP;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.json.JsonUtil;
import com.sa.common.util.StringUtils;
import com.sa.common.util.UrlUtil;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.util.RedisUtil;
import com.sa.solr.domain.Feed;

/**
 * @author lewis
 * 
 */
public class SolrQueryUtil {

	private static final Logger log = LoggerFactory.getLogger(SolrQueryUtil.class);

	public static final DateFormat df;
	public static RedisUtil redisUtil;

	static {
		df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		df.setTimeZone(TimeZone.getTimeZone("GMT0"));
		try {
			redisUtil = RedisUtil.getInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public SolrQueryUtil() {
		super();
	}

	public static StringBuilder getPolarityQuery(Set<Short> polaritySet) {
		StringBuilder sb = new StringBuilder();
		if (polaritySet.isEmpty())
			return sb;

		sb.append("+(");
		if (polaritySet.contains((short) 0)) {
			sb.append("sScore:[-0.45 TO 0.5] (*:* NOT sScore:*)");
			sb.append(" ");
		}
		if (polaritySet.contains((short) -1)) {

			sb.append("sScore:[* TO -0.45]");
			sb.append(" ");
		}

		if (polaritySet.contains((short) 1)) {
			sb.append("sScore:[0.5 TO *]");
			sb.append(" ");
		}
		sb.append(")");
		return sb;
	}

	/**
	 * filter WEB only domain, SNS domain will be added based on SNS preference
	 * 
	 * @param whiteList
	 * @param blackList
	 * @return
	 */
	public static StringBuilder getWhiteBlackList(Set<String> whiteList, Set<String> blackList) {
		StringBuilder sb = new StringBuilder();
		if (whiteList.isEmpty() && blackList.isEmpty())
			return sb;

		sb.append("+domain:(");
		// if (!snses.isEmpty()) {
		// for (Short snsId : snses) {
		// Sns sns = Sns.getSns(snsId);
		// sb.append("*").append(sns.getDomain()).append("*").append(" ");
		// }
		// }
		if (!whiteList.isEmpty()) {
			for (String domain : whiteList) {
				sb.append("*").append(domain).append("*").append(" ");
			}
		}
		if (!blackList.isEmpty()) {
			for (String domain : blackList) {
				sb.append("-").append("*").append(domain).append("*").append(" ");
			}
		}
		sb.append(") ");
		return sb;
	}

	public static StringBuilder getRegions(Set<Short> regions) {
		StringBuilder sb = new StringBuilder();
		if (null == regions || regions.isEmpty())
			return sb;

		int i = 0;
		sb.append("+(");
		for (Short region : regions) {
			if (0 == i)
				sb.append(SolrFieldDefinition.Region.getName()).append(":").append(region);
			else
				sb.append(" OR ").append(SolrFieldDefinition.Region.getName()).append(":").append(region);
			++i;
		}
		sb.append(") ");

		return sb;
	}

	public static StringBuilder getRegionsAndEmptyRegion(Set<Short> regions) {
		StringBuilder sb = new StringBuilder();
		if (null == regions || regions.isEmpty())
			return sb;

		int i = 0;
		sb.append("+(");

		sb.append("(");
		sb.append("*:* ").append("NOT ");
		sb.append(SolrFieldDefinition.Region.getName()).append(":").append("*");
		sb.append(") ");

		sb.append(SolrFieldDefinition.Region.getName()).append(":(").append(StringUtils.join(regions, " "))
				.append(") ");
		sb.append(") ");

		return sb;
	}

	public static StringBuilder getSnsAndEmptySns(Set<Short> snses) {
		StringBuilder sb = new StringBuilder();
		if (null == snses || snses.isEmpty())
			return sb;

		sb.append("+(");

		sb.append("(");
		sb.append("*:* ").append("NOT ");
		sb.append(SolrFieldDefinition.SNS_TYPE.getName()).append(":").append("*");
		sb.append(") ");

		sb.append(SolrFieldDefinition.SNS_TYPE.getName()).append(":(").append(StringUtils.join(snses, " "))
				.append(") ");
		sb.append(") ");

		return sb;
	}

	public static StringBuilder getSourceTypes(Set<Short> sourceTypes) {
		StringBuilder sb = new StringBuilder();
		if (null == sourceTypes || sourceTypes.isEmpty())
			return sb;

		sb.append("+(");
		sb.append(SolrFieldDefinition.NEW_SOURCE_TYPE.getName()).append(":(").append(StringUtils.join(sourceTypes, " "))
				.append(") ");
		sb.append(") ");

		return sb;
	}

	public static StringBuilder getSnses(Set<Short> snses) {
		StringBuilder sb = new StringBuilder();
		if (null == snses || snses.isEmpty())
			return sb;

		int i = 0;
		sb.append("+(");
		for (Short sns : snses) {
			if (0 == i)
				sb.append(SolrFieldDefinition.SNS_TYPE.getName()).append(":").append(sns);
			else
				sb.append(" OR ").append(SolrFieldDefinition.SNS_TYPE.getName()).append(":").append(sns);
			++i;
		}
		sb.append(") ");

		return sb;
	}

	public static StringBuilder getLanguages(Set<Short> languages) {
		StringBuilder sb = new StringBuilder();
		if (languages == null || languages.isEmpty()) {
			return sb;
		}
		Set<String> langSet = new HashSet<String>();
		for (Short langId : languages) {
			langSet.add(StringUtils.doubleQuote(Language.getLanguage(langId).getCode()));
		}

		sb.append("+(");
		sb.append("(");
		sb.append("*:* ").append("NOT ");
		sb.append(SolrFieldDefinition.LANGUAGE.getName()).append(":").append("*");
		sb.append(") ");

		sb.append(SolrFieldDefinition.LANGUAGE.getName()).append(":(");
		sb.append(StringUtils.join(langSet, " "));
		sb.append(")");
		sb.append(") ");
		return sb;
	}

	public static void main(String[] args) {
		System.out.println(
				combine(Sets.newHashSet((short) 1), Sets.newHashSet((short) 2), Sets.newHashSet((short) 1, (short) 3)));
	}

	public static StringBuilder combine(Set<Short> regions, Set<Short> snses, Set<Short> sourceTypes) {
		if (sourceTypes != null) {
			sourceTypes.remove(SourceType.SNS.getSourceTypeId());
		}

		StringBuilder sb = new StringBuilder();
		boolean go = (null != regions && !regions.isEmpty())
				|| (null != snses && !snses.isEmpty() || (null != sourceTypes && !sourceTypes.isEmpty()));

		if (!go)
			return sb;

		sb.append("+(");

		if (snses != null && !snses.isEmpty()) {
			sb.append("(");
			sb.append("+").append(SolrFieldDefinition.NEW_SOURCE_TYPE.getName()).append(":")
					.append(SourceType.SNS.getSourceTypeId());
			sb.append(" +").append(SolrFieldDefinition.SNS_TYPE.getName()).append(":(")
					.append(StringUtils.join(snses, " ")).append(") ");
			sb.append(") ");
		}

		if (!regions.isEmpty() || !sourceTypes.isEmpty()) {
			sb.append("(");

			if (!sourceTypes.isEmpty()) {
				if (sourceTypes.size() == 1) {
					sb.append("+").append(SolrFieldDefinition.NEW_SOURCE_TYPE.getName()).append(":")
							.append(sourceTypes.iterator().next()).append(" ");
				} else {
					sb.append("+").append(SolrFieldDefinition.NEW_SOURCE_TYPE.getName()).append(":(")
							.append(StringUtils.join(sourceTypes, " ")).append(") ");
				}
				// if (sourceTypes.size() == 1) {
				// sb.append("+").append(SolrFieldDefinition.SOURCE_TYPE.getName()).append(":").append(sourceTypes.iterator().next())
				// .append(" ");
				// } else {
				// sb.append("+").append(SolrFieldDefinition.SOURCE_TYPE.getName()).append(":(")
				// .append(StringUtils.join(sourceTypes, " ")).append(") ");
				// }
			}
			if (!regions.isEmpty()) {
				// sb.append("+").append(SolrFieldDefinition.Region.getName()).append(":(").append(StringUtils.join(regions,
				// " "))
				// .append(") ");
				sb.append(getRegionsAndEmptyRegion(regions).toString());
			}

			sb.append(")");
		}

		sb.append(") ");

		return sb;
	}

	public static StringBuilder getSourceType(String sourceType) {
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isEmpty(sourceType))
			return sb;

		sb.append("+").append(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
		sb.append(":").append(sourceType).append(" ");

		return sb;
	}

	public static StringBuilder getSnsType(String snsType) {
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isEmpty(snsType))
			return sb;

		sb.append("+").append(SolrFieldDefinition.SNS_TYPE.getName());
		sb.append(":").append(snsType).append(" ");

		return sb;
	}

	public static StringBuilder getPublishDateQuery(Date startDate, Date endDate) {
		StringBuilder sb = new StringBuilder();

		if (null != startDate || null != endDate) {
			sb.append("+").append(SolrFieldDefinition.PUBLISH_DATE.getName());
			sb.append(":[");
			sb.append(dateToSolrString(startDate));
			sb.append(" TO ");
			sb.append(dateToSolrString(endDate));
			sb.append("] ");
		}

		return sb;
	}

	public static StringBuilder getCrawlDateQuery(Date startDate, Date endDate) {
		StringBuilder sb = new StringBuilder();

		if (null != startDate || null != endDate) {
			sb.append("+").append(SolrFieldDefinition.CREATE_DATE.getName());
			sb.append(":[");
			sb.append(dateToSolrString(startDate));
			sb.append(" TO ");
			sb.append(dateToSolrString(endDate));
			sb.append("] ");
		}

		return sb;
	}

	private static String dateToSolrString(Date inDate) {
		if (inDate == null)
			return "*";
		return df.format(inDate);
	}

	public SolrDocumentList query(SolrQuery solrQ, String... collections) throws SolrServerException, IOException {

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		log.debug("SolrQuery: {}", solrQ);

		CloudSolrClient client = null;

		try {
			client = getSolrClient();
			return client.query(solrQ, METHOD.POST).getResults();
		} finally {
			if (client != null)
				client.close();
		}

	}

	public QueryResponse queryResponse(SolrQuery solrQ, String... collections) throws SolrServerException, IOException {

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		log.debug("SolrQuery: {}", solrQ);
		// String cursorMark = "*";
		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ, METHOD.POST);
		} finally {
			if (client != null)
				client.close();
		}

		return rep;
	}

	public static StringBuilder toKeywordQuery(Collection<Collection<String>> andKeywords,
			Collection<String> orKeywords) {
		StringBuilder sb = new StringBuilder();

		sb.append("+(");

		int numOfK = 0;
		int numOfandK = 0;

		if (null != orKeywords && !orKeywords.isEmpty()) {
			Set<String> finalKeywords = new HashSet<String>();
			for (String keyword : orKeywords) {
				numOfK = numOfK + 1;
				String keywordTrad = HanLP.s2hk(keyword);
				String keywordSimpl = HanLP.hk2s(keyword);
				if (!keywordSimpl.equals(keywordTrad)) {
					finalKeywords.add(keywordTrad);
					finalKeywords.add(keywordSimpl);
				} else {
					finalKeywords.add(keyword);
				}

				if (numOfK > 75) {
					break;
				}
			}

			Set<String> quotedKeywords = new HashSet<String>();
			for (String keyword : finalKeywords) {
				quotedKeywords.add(StringUtils.doubleQuote(keyword));
			}

			sb.append(StringUtils.join(quotedKeywords.toArray(new String[0]), " "));
		}

		if (null != andKeywords && !andKeywords.isEmpty()) {
			for (Collection<String> keywordSet : andKeywords) {
				numOfandK = numOfandK + 1;
				Set<String> simSet = new HashSet<String>(), tradSet = new HashSet<String>(),
						normalSet = new HashSet<String>();
				for (String keyword : keywordSet) {
					simSet.add(StringUtils.doubleQuote(HanLP.hk2s(keyword)));
					tradSet.add(StringUtils.doubleQuote(HanLP.s2hk(keyword)));
					normalSet.add(StringUtils.doubleQuote(keyword));
				}

				String keyA = StringUtils.join(simSet, " AND ");
				String keyB = StringUtils.join(tradSet, " AND ");

				if (!keyA.equals(keyB)) {
					sb.append("(").append(keyA).append(") ");
					sb.append("(").append(keyB).append(") ");
				} else {
					sb.append("(").append(StringUtils.join(normalSet, " AND ")).append(") ");
				}

				if (numOfandK > 75) {
					break;
				}
			}
		}

		sb.append(") ");

		return sb;
		// StringBuilder sb = new StringBuilder();
		// sb.append("+(title:");
		//
		// sb.append("(");
		// if (null != andKeywords && !andKeywords.isEmpty()) {
		// for (Collection<String> keywordSet : andKeywords) {
		// Set<String> simSet = new HashSet<String>(), tradSet = new
		// HashSet<String>(), normalSet = new HashSet<String>();
		// for (String keyword : keywordSet) {
		// simSet.add(StringUtils.doubleQuote(ChineseTranslator.getInstance().trad2Simpl(keyword)));
		// tradSet.add(StringUtils.doubleQuote(ChineseTranslator.getInstance().simpl2Trad(keyword)));
		// normalSet.add(StringUtils.doubleQuote(keyword));
		// }
		// sb.append("(").append(StringUtils.join(simSet, " AND ")).append(")
		// ");
		// sb.append("(").append(StringUtils.join(tradSet, " AND ")).append(")
		// ");
		// sb.append("(").append(StringUtils.join(normalSet, " AND ")).append(")
		// ");
		// }
		// }
		//
		// if (null != orKeywords && !orKeywords.isEmpty()) {
		// Set<String> finalKeywords = new HashSet<String>();
		// for (String keyword : orKeywords) {
		// finalKeywords.add(ChineseTranslator.getInstance().simpl2Trad(keyword));
		// finalKeywords.add(ChineseTranslator.getInstance().trad2Simpl(keyword));
		// finalKeywords.add(keyword);
		// }
		//
		// Set<String> quotedKeywords = new HashSet<String>();
		// for (String keyword : finalKeywords) {
		// quotedKeywords.add(StringUtils.doubleQuote(keyword));
		// }
		//
		// sb.append(StringUtils.join(quotedKeywords.toArray(new String[0]), "
		// "));
		// }
		// sb.append(") ");
		//
		// sb.append("content:");
		// sb.append("(");
		// if (null != andKeywords && !andKeywords.isEmpty()) {
		// for (Collection<String> keywordSet : andKeywords) {
		// Set<String> simSet = new HashSet<String>(), tradSet = new
		// HashSet<String>(), normalSet = new HashSet<String>();
		// for (String keyword : keywordSet) {
		// simSet.add(StringUtils.doubleQuote(ChineseTranslator.getInstance().trad2Simpl(keyword)));
		// tradSet.add(StringUtils.doubleQuote(ChineseTranslator.getInstance().simpl2Trad(keyword)));
		// normalSet.add(StringUtils.doubleQuote(keyword));
		// }
		// sb.append("(").append(StringUtils.join(simSet, " AND ")).append(")
		// ");
		// sb.append("(").append(StringUtils.join(tradSet, " AND ")).append(")
		// ");
		// sb.append("(").append(StringUtils.join(normalSet, " AND ")).append(")
		// ");
		// }
		// }
		//
		// if (null != orKeywords && !orKeywords.isEmpty()) {
		// Set<String> finalKeywords = new HashSet<String>();
		// for (String keyword : orKeywords) {
		// finalKeywords.add(ChineseTranslator.getInstance().simpl2Trad(keyword));
		// finalKeywords.add(ChineseTranslator.getInstance().trad2Simpl(keyword));
		// finalKeywords.add(keyword);
		// }
		//
		// Set<String> quotedKeywords = new HashSet<String>();
		// for (String keyword : finalKeywords) {
		// quotedKeywords.add(StringUtils.doubleQuote(keyword));
		// }
		//
		// sb.append(StringUtils.join(quotedKeywords.toArray(new String[0]), "
		// "));
		// }
		// sb.append(")");
		// sb.append(") ");
		//
		// return sb;
	}

	@Deprecated
	public static StringBuilder toOrQuery(Collection<String> orKeywords) {
		StringBuilder sb = new StringBuilder();

		if (null != orKeywords && !orKeywords.isEmpty()) {
			Set<String> finalKeywords = new HashSet<String>();
			for (String keyword : orKeywords) {
				finalKeywords.add(ChineseTranslator.getInstance().simpl2Trad(keyword));
				finalKeywords.add(ChineseTranslator.getInstance().trad2Simpl(keyword));
			}

			Set<String> quotedKeywords = new HashSet<String>();
			for (String keyword : finalKeywords) {
				quotedKeywords.add(StringUtils.doubleQuote(keyword));
			}

			sb.append("+(").append(StringUtils.join(quotedKeywords.toArray(new String[0]), " ")).append(") ");

		}

		return sb;
	}

	/**
	 * For dashboard feed display
	 * 
	 * @param solrQ
	 * @param start
	 * @param maxRecord
	 * @param highlightPre
	 * @param highlightPost
	 * @param highlightSnipperts
	 * @param hightlightFragsize
	 * @param collections
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public Pair<Long, List<Feed>> queryDashboardFeed(SolrQuery solrQ, int start, int maxRecord, String highlightPre,
			String highlightPost, int highlightSnipperts, int hightlightFragsize, String... collections)
			throws SolrServerException, IOException {
		if (start < 0)
			start = 0;

		if (maxRecord <= 0)
			maxRecord = 200;

		solrQ.setStart(start);
		solrQ.setRows(maxRecord);

		if (StringUtils.isEmpty(highlightPre) || StringUtils.isEmpty(highlightPost)) {
			highlightPre = "<em>";
			highlightPost = "</em>";
		}

		if (highlightSnipperts <= 0)
			highlightSnipperts = 1;

		if (hightlightFragsize <= 0)
			hightlightFragsize = 100;

		solrQ.setHighlight(true);
		solrQ.setHighlightSimplePre(highlightPre);
		solrQ.setHighlightSimplePost(highlightPost);
		solrQ.addHighlightField(SolrFieldDefinition.TITLE.getName());
		solrQ.addHighlightField(SolrFieldDefinition.CONTENT.getName());
		solrQ.setHighlightSnippets(highlightSnipperts);
		solrQ.setHighlightFragsize(hightlightFragsize);

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		log.info("Solr Query: {}", solrQ);

		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ);
			log.debug("{}", rep.getResults().getNumFound());
		} finally {
			if (client != null)
				client.close();
		}

		long numFound = rep.getResults().getNumFound();
		List<Feed> feeds = new ArrayList<Feed>();
		SolrDocumentList list = rep.getResults();

		if (null != list && !list.isEmpty()) {
			Map<String, Map<String, List<String>>> highlighting = rep.getHighlighting();
			for (SolrDocument doc : list) {
				String url = null;
				Object fieldValue = doc.getFieldValue(SolrFieldDefinition.URL.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					url = (String) fieldValue;
				}

				if (StringUtils.isEmpty(url))
					continue;

				Feed feed = new Feed();
				feed.setUrl(url);

				// Get publish date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setpDate((Date) fieldValue);
				}

				// Get source type name.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
				String sourceTypeName = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					sourceTypeName = SourceType.getSourceTypeName((int) fieldValue);
				}

				feed.setSourceType(sourceTypeName);

				// Get source domain.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
				String sourceDomain = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					Sns sns = Sns.getSns((int) fieldValue);
					if (null != sns)
						sourceDomain = sns.getNameEn();
				}

				if (StringUtils.isEmpty(sourceDomain)) {
					sourceDomain = UrlUtil.extractDomain(url);
				}

				feed.setSourceDomain(sourceDomain);

				Map<String, List<String>> highlightingItem = highlighting.get(url);

				// Get language.
				String langCode = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LANGUAGE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					langCode = (String) fieldValue;
					feed.setLanguage(Language.getByLangCode(langCode).getNameEn());
				}

				// Get title.
				List<String> highlightingItemValues = highlightingItem.get(SolrFieldDefinition.TITLE.getName());
				String title = null;

				if (null != highlightingItemValues && !highlightingItemValues.isEmpty()) {
					title = highlightingItemValues.get(0);
				} else {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						title = (String) fieldValue;
						if (!StringUtils.isEmpty(title) && StringUtils.length(title) > hightlightFragsize) {
							title = StringUtils.substringBeforeLast(title, highlightPost) + highlightPost + "...";
						}
					}
				}

				feed.setTitle(title);

				// Get content.
				highlightingItemValues = highlightingItem.get(SolrFieldDefinition.CONTENT.getName());
				String content = null;

				if (null != highlightingItemValues && !highlightingItemValues.isEmpty()) {
					content = highlightingItemValues.get(0);
				} else {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
					if (null != fieldValue) {
						if (fieldValue instanceof String) {
							content = (String) fieldValue;
						} else if (fieldValue instanceof Collection) {
							Collection<String> col = (Collection<String>) fieldValue;
							if (null != col && !col.isEmpty()) {
								for (String str : col) {
									content = str;
									break;
								}
							}
						}

					}
				}

				if (!StringUtils.isEmpty(content) && StringUtils.length(content) > hightlightFragsize) {
					content = StringUtils.substringBeforeLast(content, highlightPost) + highlightPost + "...";
				}

				if (content != null && content.length() > 500) {
					String before = content.substring(0, 100);
					String after = content.substring(content.length() - 100);
					content = before + "..." + after;
				}

				feed.setContent(content);

				// Get photo.
				String photo = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.IMAGES.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String img : col) {
								photo = img;
								break;
							}
						}
					} else if (fieldValue instanceof String) {
						photo = (String) fieldValue;
					}

				}

				if (null == photo) {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.PRIMARY_IMAGE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				feed.setPhoto(photo);

				// Get views.
				Long views = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					views = (Long) fieldValue;
				}
				feed.setViews(views);

				// Get likes.
				Long likes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					likes = (Long) fieldValue;
				}
				feed.setLikes(likes);

				// Get comments.
				Long comments = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					comments = (Long) fieldValue;
				}
				feed.setComments(comments);

				// Get shares.
				Long shares = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					shares = (Long) fieldValue;
				}
				feed.setShares(shares);

				// Get dislikes.
				Long dislikes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					dislikes = (Long) fieldValue;
				}
				feed.setDislikes(dislikes);

				// Get user id.
				String uId = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.USER_ID.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					uId = (String) fieldValue;
				}

				feed.setuId(uId);

				// Get post id.
				String postId = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.TWEET_ID.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					postId = (String) fieldValue;
				}

				if (null != postId) {
					feed.setPostId(postId);
				} else {
					feed.setPostId(DigestUtils.md5Hex(url));
				}

				feed.setFeedId(DigestUtils.md5Hex(url));

				if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
					feed.setsScore((Float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.AUTHOR.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.AUTHOR.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.Region.getName())) {
					Integer regionId = (Integer) doc.getFieldValue(SolrFieldDefinition.Region.getName());
					feed.setRegion(Region.getRegionById(regionId).getNameEn());
				}

				String regionCat = redisUtil.hget(RedisDefinition.TaskManagerDef.DOMAIN_CATEGORY_UPDATE, sourceDomain);
				// log.info("update domain======================"+sourceDomain);
				if (null != regionCat) {
					Integer newRegionId = Integer.valueOf(regionCat);
					// log.info("update
					// domain======================"+sourceDomain+"regionId============"+newRegionId);
					feed.setRegion(Region.getRegionById(newRegionId).getNameEn());
				}

				Set<String> discoveredKwSet = new HashSet<String>();
				if (doc.containsKey(SolrFieldDefinition.POS_THEMES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.POS_THEMES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.NEG_THEMES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.NEG_THEMES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.NEUTRAL_THEMES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.NEUTRAL_THEMES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.PLACES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.PLACES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.COMPANIES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.COMPANIES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.PERSONS.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.PERSONS.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.PRODUCTS.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.PRODUCTS.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (!discoveredKwSet.isEmpty()) {
					feed.setDiscoveredKeywords(discoveredKwSet);
				}

				/**
				 * Reaction
				 */
				if (doc.containsKey(SolrFieldDefinition.FB_ANGRY.getName())) {
					feed.setAngryCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_ANGRY.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_LOVE.getName())) {
					feed.setLoveCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_LOVE.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_SAD.getName())) {
					feed.setSadCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_SAD.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_HAHA.getName())) {
					feed.setHahaCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_HAHA.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_THANKFUL.getName())) {
					feed.setThankfulCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_THANKFUL.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_WOW.getName())) {
					feed.setWowCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_WOW.getName()));
				}

				// ptt fields
				if (doc.containsKey(SolrFieldDefinition.PTT_JIAN.getName())) {
					feed.setPttJianCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_JIAN.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_XU.getName())) {
					feed.setPttXuCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_XU.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_TUI.getName())) {
					feed.setPttTuiCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_TUI.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_LOCATION.getName())) {
					feed.setPttLocation((String) doc.getFieldValue(SolrFieldDefinition.PTT_LOCATION.getName()));
				}

				feeds.add(feed);

			}
		}

		return Pair.of(numFound, feeds);
	}

	/**
	 * Query with multiple keywords using OR operator
	 * 
	 * @param SolrQuery
	 * @param sortBy
	 * @param order
	 * @param fields
	 * @param start
	 * @param maxRecord
	 * @param highlightPre
	 * @param highlightPost
	 * @param highlightSnipperts
	 * @param hightlightFragsize
	 * @param collections
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */

	public Pair<Long, List<Feed>> query(SolrQuery solrQ, int start, int maxRecord, String highlightPre,
			String highlightPost, int highlightSnipperts, int hightlightFragsize, String... collections)
			throws SolrServerException, IOException {

		if (start < 0)
			start = 0;

		if (maxRecord <= 0)
			maxRecord = 200;

		solrQ.setStart(start);
		solrQ.setRows(maxRecord);

		if (StringUtils.isEmpty(highlightPre) || StringUtils.isEmpty(highlightPost)) {
			highlightPre = "<em>";
			highlightPost = "</em>";
		}

		if (highlightSnipperts <= 0)
			highlightSnipperts = 1;

		if (hightlightFragsize <= 0)
			hightlightFragsize = 100;

		solrQ.setHighlight(true);
		solrQ.setHighlightSimplePre(highlightPre);
		solrQ.setHighlightSimplePost(highlightPost);
		solrQ.addHighlightField(SolrFieldDefinition.TITLE.getName());
		solrQ.addHighlightField(SolrFieldDefinition.CONTENT.getName());
		solrQ.setHighlightSnippets(highlightSnipperts);
		solrQ.setHighlightFragsize(hightlightFragsize);

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		log.info("Solr Query: {}", solrQ);

		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ, METHOD.POST);
			log.debug("{}", rep.getResults().getNumFound());
		} finally {
			if (client != null)
				client.close();
		}

		long numFound = rep.getResults().getNumFound();
		List<Feed> feeds = new ArrayList<Feed>();
		SolrDocumentList list = rep.getResults();

		if (null != list && !list.isEmpty()) {
			Map<String, Map<String, List<String>>> highlighting = rep.getHighlighting();
			for (SolrDocument doc : list) {
				String url = null;
				Object fieldValue = doc.getFieldValue(SolrFieldDefinition.URL.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					url = (String) fieldValue;
				}

				if (StringUtils.isEmpty(url))
					continue;

				Feed feed = new Feed();
				feed.setUrl(url);
				feed.setLanguage("Simplified Chinese");

				// Get publish date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setpDate((Date) fieldValue);
				}

				// Get crawler date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.CREATE_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setcDate((Date) fieldValue);
				}

				// Get create date
				fieldValue = doc.getFieldValue(SolrFieldDefinition.CREATE_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setcDate((Date) fieldValue);
				}

				// Get source type name.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
				String sourceTypeName = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					sourceTypeName = SourceType.getSourceTypeName((int) fieldValue);
				}

				feed.setSourceType(sourceTypeName);

				// Get source domain.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
				String sourceDomain = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					Sns sns = Sns.getSns((int) fieldValue);
					if (null != sns)
						sourceDomain = sns.getNameEn();
				}

				if (StringUtils.isEmpty(sourceDomain)) {
					sourceDomain = UrlUtil.extractDomain(url);
				}

				feed.setSourceDomain(sourceDomain);

				Map<String, List<String>> highlightingItem = highlighting.get(url);

				// Get language.
				String langCode = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LANGUAGE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					langCode = (String) fieldValue;
					feed.setLanguage(Language.getByLangCode(langCode).getNameEn());
				}

				// Get title.
				List<String> highlightingItemValues = highlightingItem.get(SolrFieldDefinition.TITLE.getName());
				String title = null;

				if (null != highlightingItemValues && !highlightingItemValues.isEmpty()) {
					title = highlightingItemValues.get(0);
				} else {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						title = (String) fieldValue;
						if (!StringUtils.isEmpty(title) && StringUtils.length(title) > hightlightFragsize) {
							title = StringUtils.substringBeforeLast(title, highlightPost) + highlightPost + "...";
						}
					}
				}

				feed.setTitle(title);

				// Get content.
				highlightingItemValues = highlightingItem.get(SolrFieldDefinition.CONTENT.getName());
				String content = null;

				if (null != highlightingItemValues && !highlightingItemValues.isEmpty()) {
					content = highlightingItemValues.get(0);
				} else {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
					if (null != fieldValue) {
						if (fieldValue instanceof String) {
							content = (String) fieldValue;
						} else if (fieldValue instanceof Collection) {
							Collection<String> col = (Collection<String>) fieldValue;
							if (null != col && !col.isEmpty()) {
								for (String str : col) {
									content = str;
									break;
								}
							}
						}
					}
				}

				if (!StringUtils.isEmpty(content) && StringUtils.length(content) > hightlightFragsize) {
					content = StringUtils.substringBeforeLast(content, highlightPost) + highlightPost + "...";
				}

				if (content != null && content.length() > 500) {
					String before = content.substring(0, 100);
					String after = content.substring(content.length() - 100);
					content = before + "..." + after;
				}

				feed.setContent(content);

				// Get photo.
				String photo = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.IMAGES.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String img : col) {
								photo = img;
								break;
							}
						}
					} else if (fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				if (null == photo) {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.PRIMARY_IMAGE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				feed.setPhoto(photo);

				// Get views.
				Long views = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					views = (Long) fieldValue;
				}
				feed.setViews(views);

				// Get likes.
				Long likes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					likes = (Long) fieldValue;
				}
				feed.setLikes(likes);

				// Get comments.
				Long comments = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					comments = (Long) fieldValue;
				}
				feed.setComments(comments);

				// Get shares.
				Long shares = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					shares = (Long) fieldValue;
				}
				feed.setShares(shares);

				// Get dislikes.
				Long dislikes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					dislikes = (Long) fieldValue;
				}
				feed.setDislikes(dislikes);

				if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
					feed.setsScore((Float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.AUTHOR.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.AUTHOR.getName()));
				}

				feed.setFeedId(DigestUtils.md5Hex(url));

				if (doc.containsKey(SolrFieldDefinition.Region.getName())) {
					Integer regionId = (Integer) doc.getFieldValue(SolrFieldDefinition.Region.getName());
					feed.setRegion(Region.getRegionById(regionId).getNameEn());
				}

				// ptt fields
				if (doc.containsKey(SolrFieldDefinition.PTT_JIAN.getName())) {
					feed.setPttJianCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_JIAN.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_XU.getName())) {
					feed.setPttXuCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_XU.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_TUI.getName())) {
					feed.setPttTuiCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_TUI.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_LOCATION.getName())) {
					feed.setPttLocation((String) doc.getFieldValue(SolrFieldDefinition.PTT_LOCATION.getName()));
				}

				feeds.add(feed);
			}
		}
		if(client != null){
			client.close();
		}

		return Pair.of(numFound, feeds);
	}

	public Pair<Long, List<Feed>> queryFullText(SolrQuery solrQ, int start, int maxRecord, String highlightPre,
			String highlightPost, int highlightSnipperts, int hightlightFragsize, String... collections)
			throws SolrServerException, IOException {

		if (start < 0)
			start = 0;

		if (maxRecord <= 0)
			maxRecord = 200;

		solrQ.setStart(start);
		solrQ.setRows(maxRecord);

		if (StringUtils.isEmpty(highlightPre) || StringUtils.isEmpty(highlightPost)) {
			highlightPre = "<em>";
			highlightPost = "</em>";
		}

		if (highlightSnipperts <= 0)
			highlightSnipperts = 1;

		if (hightlightFragsize <= 0)
			hightlightFragsize = 100;

		solrQ.setHighlight(true);
		solrQ.setHighlightSimplePre(highlightPre);
		solrQ.setHighlightSimplePost(highlightPost);
		solrQ.addHighlightField(SolrFieldDefinition.TITLE.getName());
		solrQ.addHighlightField(SolrFieldDefinition.CONTENT.getName());
		solrQ.setHighlightSnippets(highlightSnipperts);
		solrQ.setHighlightFragsize(hightlightFragsize);

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		log.info("Solr Query: {}", solrQ);

		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ, METHOD.POST);
			log.debug("{}", rep.getResults().getNumFound());
			client.getZkStateReader().close();
		} finally {
			if (client != null)
				client.close();
		}

		long numFound = rep.getResults().getNumFound();
		List<Feed> feeds = new ArrayList<Feed>();
		SolrDocumentList list = rep.getResults();

		if (null != list && !list.isEmpty()) {
			Map<String, Map<String, List<String>>> highlighting = rep.getHighlighting();
			for (SolrDocument doc : list) {
				String url = null;
				Object fieldValue = doc.getFieldValue(SolrFieldDefinition.URL.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					url = (String) fieldValue;
				}

				if (StringUtils.isEmpty(url))
					continue;

				Feed feed = new Feed();
				feed.setUrl(url);
				feed.setLanguage("Simplified Chinese");

				// Get publish date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setpDate((Date) fieldValue);
				}

				// Get crawler date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.CREATE_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setcDate((Date) fieldValue);
				}

				// Get create date
				fieldValue = doc.getFieldValue(SolrFieldDefinition.CREATE_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setcDate((Date) fieldValue);
				}

				// Get source type name.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
				String sourceTypeName = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					sourceTypeName = SourceType.getSourceTypeName((int) fieldValue);
				}

				feed.setSourceType(sourceTypeName);

				// Get source domain.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
				String sourceDomain = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					Sns sns = Sns.getSns((int) fieldValue);
					if (null != sns)
						sourceDomain = sns.getNameEn();
				}

				if (StringUtils.isEmpty(sourceDomain)) {
					sourceDomain = UrlUtil.extractDomain(url);
				}

				feed.setSourceDomain(sourceDomain);

				Map<String, List<String>> highlightingItem = highlighting.get(url);

				// Get language.
				String langCode = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LANGUAGE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					langCode = (String) fieldValue;
					feed.setLanguage(Language.getByLangCode(langCode).getNameEn());
				}

				String title = null;

				fieldValue = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					title = (String) fieldValue;
					if (!StringUtils.isEmpty(title) && StringUtils.length(title) > hightlightFragsize) {
						title = StringUtils.substringBeforeLast(title, highlightPost) + highlightPost + "...";
					}
				}

				feed.setTitle(title);

				String content = null;

				fieldValue = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof String) {
						content = (String) fieldValue;
					} else if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String str : col) {
								content = str;
								break;
							}
						}
					}
				}

				if (!StringUtils.isEmpty(content) && StringUtils.length(content) > hightlightFragsize) {
					content = StringUtils.substringBeforeLast(content, highlightPost) + highlightPost + "...";
				}

				if (content != null && content.length() > 500) {
					String before = content.substring(0, 100);
					String after = content.substring(content.length() - 100);
					content = before + "..." + after;
				}

				feed.setContent(content);

				// Get photo.
				String photo = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.IMAGES.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String img : col) {
								photo = img;
								break;
							}
						}
					} else if (fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				if (null == photo) {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.PRIMARY_IMAGE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				feed.setPhoto(photo);

				// Get views.
				Long views = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					views = (Long) fieldValue;
				}
				feed.setViews(views);

				// Get likes.
				Long likes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					likes = (Long) fieldValue;
				}
				feed.setLikes(likes);

				// Get comments.
				Long comments = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					comments = (Long) fieldValue;
				}
				feed.setComments(comments);

				// Get shares.
				Long shares = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					shares = (Long) fieldValue;
				}
				feed.setShares(shares);

				// Get dislikes.
				Long dislikes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					dislikes = (Long) fieldValue;
				}
				feed.setDislikes(dislikes);

				if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
					feed.setsScore((Float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.AUTHOR.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.AUTHOR.getName()));
				}

				feed.setFeedId(DigestUtils.md5Hex(url));

				if (doc.containsKey(SolrFieldDefinition.Region.getName())) {
					Integer regionId = (Integer) doc.getFieldValue(SolrFieldDefinition.Region.getName());
					feed.setRegion(Region.getRegionById(regionId).getNameEn());
				}

				// ptt fields
				if (doc.containsKey(SolrFieldDefinition.PTT_JIAN.getName())) {
					feed.setPttJianCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_JIAN.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_XU.getName())) {
					feed.setPttXuCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_XU.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_TUI.getName())) {
					feed.setPttTuiCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_TUI.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_LOCATION.getName())) {
					feed.setPttLocation((String) doc.getFieldValue(SolrFieldDefinition.PTT_LOCATION.getName()));
				}

				feeds.add(feed);
			}
		}
		if(client != null){
			client.close();
		}

		return Pair.of(numFound, feeds);
	}
	
	public List<Feed> queryFullText(SolrQuery solrQ, int start, int maxRecord)
			throws SolrServerException, IOException {

		if (start < 0)
			start = 0;

		if (maxRecord <= 0)
			maxRecord = 200;

		solrQ.setStart(start);
		solrQ.setRows(maxRecord);

		solrQ.add("collection", getAllCollection());


		log.info("Solr Query: {}", solrQ);

		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ, METHOD.POST);
			log.debug("{}", rep.getResults().getNumFound());
			client.getZkStateReader().close();
		} finally {
			if (client != null)
				client.close();
		}

		long numFound = rep.getResults().getNumFound();
		List<Feed> feeds = new ArrayList<Feed>();
		SolrDocumentList list = rep.getResults();

		if (null != list && !list.isEmpty()) {
			for (SolrDocument doc : list) {
				String url = null;
				Object fieldValue = doc.getFieldValue(SolrFieldDefinition.URL.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					url = (String) fieldValue;
				}

				if (StringUtils.isEmpty(url))
					continue;

				Feed feed = new Feed();
				feed.setUrl(url);
				feed.setLanguage("Simplified Chinese");

				// Get publish date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setpDate((Date) fieldValue);
				}

				// Get crawler date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.CREATE_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setcDate((Date) fieldValue);
				}

				// Get create date
				fieldValue = doc.getFieldValue(SolrFieldDefinition.CREATE_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setcDate((Date) fieldValue);
				}

				// Get source type name.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
				String sourceTypeName = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					sourceTypeName = SourceType.getSourceTypeName((int) fieldValue);
				}

				feed.setSourceType(sourceTypeName);

				// Get source domain.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
				String sourceDomain = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					Sns sns = Sns.getSns((int) fieldValue);
					if (null != sns)
						sourceDomain = sns.getNameEn();
				}

				if (StringUtils.isEmpty(sourceDomain)) {
					sourceDomain = UrlUtil.extractDomain(url);
				}

				feed.setSourceDomain(sourceDomain);

				// Get language.
				String langCode = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LANGUAGE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					langCode = (String) fieldValue;
					feed.setLanguage(Language.getByLangCode(langCode).getNameEn());
				}

				String title = null;

				fieldValue = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					title = (String) fieldValue;
				}

				feed.setTitle(title);

				String content = null;

				fieldValue = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof String) {
						content = (String) fieldValue;
					} else if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String str : col) {
								content = str;
								break;
							}
						}
					}
				}

				feed.setContent(content);

				// Get photo.
				String photo = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.IMAGES.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String img : col) {
								photo = img;
								break;
							}
						}
					} else if (fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				if (null == photo) {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.PRIMARY_IMAGE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				feed.setPhoto(photo);

				// Get views.
				Long views = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					views = (Long) fieldValue;
				}
				feed.setViews(views);

				// Get likes.
				Long likes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					likes = (Long) fieldValue;
				}
				feed.setLikes(likes);

				// Get comments.
				Long comments = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					comments = (Long) fieldValue;
				}
				feed.setComments(comments);

				// Get shares.
				Long shares = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					shares = (Long) fieldValue;
				}
				feed.setShares(shares);

				// Get dislikes.
				Long dislikes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					dislikes = (Long) fieldValue;
				}
				feed.setDislikes(dislikes);

				if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
					feed.setsScore((Float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.AUTHOR.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.AUTHOR.getName()));
				}

				feed.setFeedId(DigestUtils.md5Hex(url));

				if (doc.containsKey(SolrFieldDefinition.Region.getName())) {
					Integer regionId = (Integer) doc.getFieldValue(SolrFieldDefinition.Region.getName());
					feed.setRegion(Region.getRegionById(regionId).getNameEn());
				}

				// ptt fields
				if (doc.containsKey(SolrFieldDefinition.PTT_JIAN.getName())) {
					feed.setPttJianCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_JIAN.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_XU.getName())) {
					feed.setPttXuCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_XU.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_TUI.getName())) {
					feed.setPttTuiCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_TUI.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_LOCATION.getName())) {
					feed.setPttLocation((String) doc.getFieldValue(SolrFieldDefinition.PTT_LOCATION.getName()));
				}

				feeds.add(feed);
			}
		}
		if(client != null){
			client.close();
		}

		return feeds;
	}

	/**
	 * Query with multiple keywords using OR operator
	 * 
	 * @param orKeywords
	 * @param queryString
	 * @param regions
	 * @param sourceType
	 * @param snsType
	 * @param dateStart
	 * @param dateEnd
	 * @param excluded
	 * @param sortBy
	 * @param order
	 * @param fields
	 * @param start
	 * @param maxRecord
	 * @param highlightPre
	 * @param highlightPost
	 * @param highlightSnipperts
	 * @param hightlightFragsize
	 * @param collections
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */

	public Pair<Long, List<Feed>> query(Collection<Collection<String>> andKeywords, Collection<String> orKeywords,
			String queryString, Set<Short> regions, String sourceType, String snsType, Date dateStart, Date dateEnd,
			Collection<String> excluded, SolrFieldDefinition sortBy, ORDER order, SolrFieldDefinition[] fields,
			int start, int maxRecord, String highlightPre, String highlightPost, int highlightSnipperts,
			int hightlightFragsize, String... collections) throws SolrServerException, IOException {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getRegionsAndEmptyRegion(regions));
		query.append(getSourceType(sourceType));
		query.append(getSnsType(snsType));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		// log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());

		if (null == sortBy)
			sortBy = SolrFieldDefinition.PUBLISH_DATE;

		if (null == order)
			order = ORDER.desc;

		solrQ.addSort(sortBy.getName(), order);

		if (start < 0)
			start = 0;

		if (maxRecord <= 0)
			maxRecord = 200;

		solrQ.setStart(start);
		solrQ.setRows(maxRecord);

		if (StringUtils.isEmpty(highlightPre) || StringUtils.isEmpty(highlightPost)) {
			highlightPre = "<em>";
			highlightPost = "</em>";
		}

		if (highlightSnipperts <= 0)
			highlightSnipperts = 1;

		if (hightlightFragsize <= 0)
			hightlightFragsize = 100;

		solrQ.setHighlight(true);
		solrQ.setHighlightSimplePre(highlightPre);
		solrQ.setHighlightSimplePost(highlightPost);
		solrQ.addHighlightField(SolrFieldDefinition.TITLE.getName());
		solrQ.addHighlightField(SolrFieldDefinition.CONTENT.getName());
		solrQ.setHighlightSnippets(highlightSnipperts);
		solrQ.setHighlightFragsize(hightlightFragsize);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		log.info("Solr Query: {}", solrQ);

		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ);
			log.debug("{}", rep.getResults().getNumFound());
		} finally {
			if (client != null)
				client.close();
		}

		long numFound = rep.getResults().getNumFound();
		List<Feed> feeds = new ArrayList<Feed>();
		SolrDocumentList list = rep.getResults();

		if (null != list && !list.isEmpty()) {
			Map<String, Map<String, List<String>>> highlighting = rep.getHighlighting();
			for (SolrDocument doc : list) {
				String url = null;
				Object fieldValue = doc.getFieldValue(SolrFieldDefinition.URL.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					url = (String) fieldValue;
				}

				if (StringUtils.isEmpty(url))
					continue;

				Feed feed = new Feed();
				feed.setUrl(url);

				// Get publish date.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if (null != fieldValue && fieldValue instanceof Date) {
					feed.setpDate((Date) fieldValue);
				}

				// Get source type name.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
				String sourceTypeName = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					sourceTypeName = SourceType.getSourceTypeName((int) fieldValue);
				}

				feed.setSourceType(sourceTypeName);

				// Get source domain.
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
				String sourceDomain = null;
				if (null != fieldValue && fieldValue instanceof Integer) {
					Sns sns = Sns.getSns((int) fieldValue);
					if (null != sns)
						sourceDomain = sns.getNameEn();
				}

				if (StringUtils.isEmpty(sourceDomain)) {
					sourceDomain = UrlUtil.extractDomain(url);
				}

				feed.setSourceDomain(sourceDomain);

				Map<String, List<String>> highlightingItem = highlighting.get(url);

				// Get language.
				String langCode = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LANGUAGE.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					langCode = (String) fieldValue;
					feed.setLanguage(Language.getByLangCode(langCode).getNameEn());
				}

				// Get title.
				List<String> highlightingItemValues = highlightingItem.get(SolrFieldDefinition.TITLE.getName());
				String title = null;

				if (null != highlightingItemValues && !highlightingItemValues.isEmpty()) {
					title = highlightingItemValues.get(0);
				} else {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						title = (String) fieldValue;
						if (!StringUtils.isEmpty(title) && StringUtils.length(title) > hightlightFragsize) {
							title = StringUtils.substringBeforeLast(title, highlightPost) + highlightPost + "...";
						}
					}
				}

				feed.setTitle(title);

				// Get content.
				highlightingItemValues = highlightingItem.get(SolrFieldDefinition.CONTENT.getName());
				String content = null;

				if (null != highlightingItemValues && !highlightingItemValues.isEmpty()) {
					content = highlightingItemValues.get(0);
				} else {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
					if (null != fieldValue) {
						if (fieldValue instanceof String) {
							content = (String) fieldValue;
						} else if (fieldValue instanceof Collection) {
							Collection<String> col = (Collection<String>) fieldValue;
							if (null != col && !col.isEmpty()) {
								for (String str : col) {
									content = str;
									break;
								}
							}
						}

					}
				}

				if (!StringUtils.isEmpty(content) && StringUtils.length(content) > hightlightFragsize) {
					content = StringUtils.substringBeforeLast(content, highlightPost) + highlightPost + "...";
				}

				if (content != null && content.length() > 500) {
					String before = content.substring(0, 100);
					String after = content.substring(content.length() - 100);
					content = before + "..." + after;
				}

				feed.setContent(content);

				// Get photo.
				String photo = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.IMAGES.getName());
				if (null != fieldValue) {
					if (fieldValue instanceof Collection) {
						Collection<String> col = (Collection<String>) fieldValue;
						if (null != col && !col.isEmpty()) {
							for (String img : col) {
								photo = img;
								break;
							}
						}
					} else if (fieldValue instanceof String) {
						photo = (String) fieldValue;
					}

				}

				if (null == photo) {
					fieldValue = doc.getFieldValue(SolrFieldDefinition.PRIMARY_IMAGE.getName());
					if (null != fieldValue && fieldValue instanceof String) {
						photo = (String) fieldValue;
					}
				}

				feed.setPhoto(photo);

				// Get views.
				Long views = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					views = (Long) fieldValue;
				}
				feed.setViews(views);

				// Get likes.
				Long likes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					likes = (Long) fieldValue;
				}
				feed.setLikes(likes);

				// Get comments.
				Long comments = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					comments = (Long) fieldValue;
				}
				feed.setComments(comments);

				// Get shares.
				Long shares = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					shares = (Long) fieldValue;
				}
				feed.setShares(shares);

				// Get dislikes.
				Long dislikes = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
				if (null != fieldValue && fieldValue instanceof Long) {
					dislikes = (Long) fieldValue;
				}
				feed.setDislikes(dislikes);

				// Get user id.
				String uId = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.USER_ID.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					uId = (String) fieldValue;
				}

				feed.setuId(uId);

				// Get post id.
				String postId = null;
				fieldValue = doc.getFieldValue(SolrFieldDefinition.TWEET_ID.getName());
				if (null != fieldValue && fieldValue instanceof String) {
					postId = (String) fieldValue;
				}

				if (null != postId) {
					feed.setPostId(postId);
				} else {
					feed.setPostId(DigestUtils.md5Hex(url));
				}

				feed.setFeedId(DigestUtils.md5Hex(url));

				if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
					feed.setsScore((Float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName())) {
					feed.setAuthor((String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName()));
				}

				if (doc.containsKey(SolrFieldDefinition.Region.getName())) {
					Integer regionId = (Integer) doc.getFieldValue(SolrFieldDefinition.Region.getName());
					feed.setRegion(Region.getRegionById(regionId).getNameEn());
				}

				String regionCat = redisUtil.hget(RedisDefinition.TaskManagerDef.DOMAIN_CATEGORY_UPDATE, sourceDomain);
				// log.info("update domain======================"+sourceDomain);
				if (null != regionCat) {
					Integer newRegionId = Integer.valueOf(regionCat);
					// log.info("update
					// domain======================"+sourceDomain+"regionId============"+newRegionId);
					feed.setRegion(Region.getRegionById(newRegionId).getNameEn());
				}

				Set<String> discoveredKwSet = new HashSet<String>();
				if (doc.containsKey(SolrFieldDefinition.POS_THEMES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.POS_THEMES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.NEG_THEMES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.NEG_THEMES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.NEUTRAL_THEMES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.NEUTRAL_THEMES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.PLACES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.PLACES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.COMPANIES.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.COMPANIES.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.PERSONS.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.PERSONS.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (doc.containsKey(SolrFieldDefinition.PRODUCTS.getName())) {
					List<String> discoveredKeywords = (List<String>) doc
							.getFieldValue(SolrFieldDefinition.PRODUCTS.getName());
					discoveredKwSet.addAll(discoveredKeywords);
				}
				if (!discoveredKwSet.isEmpty()) {
					feed.setDiscoveredKeywords(discoveredKwSet);
				}

				/**
				 * Reaction
				 */
				if (doc.containsKey(SolrFieldDefinition.FB_ANGRY.getName())) {
					feed.setAngryCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_ANGRY.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_LOVE.getName())) {
					feed.setLoveCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_LOVE.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_SAD.getName())) {
					feed.setSadCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_SAD.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_HAHA.getName())) {
					feed.setHahaCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_HAHA.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_THANKFUL.getName())) {
					feed.setThankfulCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_THANKFUL.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.FB_WOW.getName())) {
					feed.setWowCnt((Long) doc.getFieldValue(SolrFieldDefinition.FB_WOW.getName()));
				}

				// ptt fields
				if (doc.containsKey(SolrFieldDefinition.PTT_JIAN.getName())) {
					feed.setPttJianCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_JIAN.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_XU.getName())) {
					feed.setPttXuCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_XU.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_TUI.getName())) {
					feed.setPttTuiCnt((Long) doc.getFieldValue(SolrFieldDefinition.PTT_TUI.getName()));
				}
				if (doc.containsKey(SolrFieldDefinition.PTT_LOCATION.getName())) {
					feed.setPttLocation((String) doc.getFieldValue(SolrFieldDefinition.PTT_LOCATION.getName()));
				}

				feeds.add(feed);

			}
		}

		return Pair.of(numFound, feeds);

	}

	public SolrDocumentList ExportQuery(Set<Short> regions, Set<Short> sourceTypes, Set<Short> snses,
			Collection<Collection<String>> andKeywords, Collection<String> orKeywords, String queryString,
			Date dateStart, Date dateEnd, Collection<String> excluded, SolrFieldDefinition[] fields, int rows,
			int start, String... collections) throws SolrServerException, IOException {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		if (null != regions && null != snses && null != sourceTypes) {
			query.append(combine(regions, snses, sourceTypes));
		}
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
		solrQ.setStart(start);
		solrQ.setRows(rows);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		CloudSolrClient client = null;
		QueryResponse rep;

		try {
			client = getSolrClient();
			rep = client.query(solrQ);
			log.info("Number {}", rep.getResults().getNumFound());
		} finally {
			if (client != null)
				client.close();
		}

		SolrDocumentList list = rep.getResults();

		return list;
	}

	public static String excludedQuery(Collection<String> excluded) {
		log.debug("Adding excluded keywords.");
		Set<String> keywords = new HashSet<String>();
		if (excluded != null) {
			int numOfKey = 0;
			for (String keyword : excluded) {
				keywords.add(StringUtils.join("-\"", ChineseTranslator.getInstance().simpl2Trad(keyword), "\""));
				keywords.add(StringUtils.join("-\"", ChineseTranslator.getInstance().trad2Simpl(keyword), "\""));
				numOfKey++;

				if (numOfKey > 50) {
					break;
				}
			}
		}

		String ex = StringUtils.join(keywords.toArray(new String[] {}), " ");
		log.debug("Excluded query: {}", ex);
		return ex;
	}

	protected CloudSolrClient getSolrClient() {
		return SolrClientManager.getSolrClient(null);
	}

	public static String getAllCollection() {
		return SolrCollection.getAllCollectionString();
	}

	@Deprecated
	public static SolrQuery getSolrQueryOr(Collection<String> keywords, String queryString, Date dateStart,
			Date dateEnd, Collection<String> excluded, SolrFieldDefinition[] fields, String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		query.append(toOrQuery(keywords));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
		solrQ.setRows(4000);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}

	public static SolrQuery getSolrQuery(Collection<Collection<String>> andKeywords, Collection<String> orKeywords,
			Set<Short> sourceTypes, Set<Short> snses, String queryString, Date dateStart, Date dateEnd,
			Collection<String> excluded, SolrFieldDefinition[] fields, String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		if (null != snses) {
			query.append(combine(SetUtils.EMPTY_SET, snses, sourceTypes));
		}
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
		solrQ.setRows(3000);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}

	public static SolrQuery getSolrQuery(Collection<Collection<String>> andKeywords, Collection<String> orKeywords,
			Set<Short> sourceTypes, Set<Short> snses, Set<Short> regions, String queryString, Date dateStart,
			Date dateEnd, Collection<String> excluded, SolrFieldDefinition[] fields, String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		if (null != snses) {
			query.append(combine(regions, snses, sourceTypes));
		}

		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		// solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
		solrQ.setRows(3000);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}

	public SolrQuery getCDateSolrQuery(Collection<Collection<String>> andKeywords, Collection<String> orKeywords,
			Set<Short> sourceTypes, Set<Short> snses, Set<Short> regions, String queryString, Date dateStart,
			Date dateEnd, Collection<String> excluded, SolrFieldDefinition[] fields, String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		if (null != snses) {
			query.append(combine(regions, snses, sourceTypes));
		}
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getCrawlDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
		solrQ.setRows(3000);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}

	public SolrQuery getSolrQueryFeeds(Collection<Collection<String>> andKeywords, Collection<String> orKeywords,
			Set<Short> sourceTypes, Set<Short> snses, Set<Short> regions, String queryString, Date dateStart,
			Date dateEnd, Collection<String> excluded, SolrFieldDefinition[] fields, String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		query.append(getSnsAndEmptySns(snses));
		query.append(getRegionsAndEmptyRegion(regions));
		query.append(getSourceTypes(sourceTypes));
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}

	/**
	 * Compose SolrQuery for doing Spark analysis
	 * 
	 * @param andKeywords
	 * @param orKeywords
	 * @param sourceTypes
	 * @param snses
	 * @param queryString
	 * @param dateStart
	 * @param dateEnd
	 * @param excluded
	 * @param fields
	 * @param collections
	 * @return
	 */
	public static SolrQuery getSolrQueryForSpark(Collection<Collection<String>> andKeywords,
			Collection<String> orKeywords, Set<Short> sourceTypes, Set<Short> snses, Set<Short> regions,
			String queryString, Date dateStart, Date dateEnd, Collection<String> excluded, SolrFieldDefinition[] fields,
			String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		if (null != snses) {
			query.append(combine(regions, snses, sourceTypes));
		}
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(getPublishDateQuery(dateStart, dateEnd));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}

	public static SolrQuery getSolrQueryForSpark(Collection<Collection<String>> andKeywords,
			Collection<String> orKeywords, Set<Short> sourceTypes, Set<Short> snses, Set<Short> regions,
			String queryString, Collection<String> excluded, SolrFieldDefinition[] fields, String... collections) {

		SolrQuery solrQ = new SolrQuery();
		StringBuilder query = new StringBuilder();
		if (null != snses) {
			query.append(combine(regions, snses, sourceTypes));
		}
		query.append(toKeywordQuery(andKeywords, orKeywords));
		query.append(StringUtils.defaultString(queryString));

		if (excluded != null && !excluded.isEmpty()) {
			query.append(excludedQuery(excluded)).append(" ");
		}

		log.debug("Query string: {}", query.toString());
		solrQ.setQuery(query.toString());
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);

		if (fields != null)
			for (SolrFieldDefinition field : fields) {
				solrQ.addField(field.getName());
			}

		if (collections == null || collections.length == 0) {
			solrQ.add("collection", getAllCollection());
		} else {
			solrQ.add("collection", StringUtils.join(collections, ","));
		}

		return solrQ;
	}
}
