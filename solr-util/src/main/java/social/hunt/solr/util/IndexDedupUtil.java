/**
 * 
 */
package social.hunt.solr.util;

import static com.sa.common.definition.SolrFieldDefinition.DOMAIN;
import static com.sa.common.definition.SolrFieldDefinition.URL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.common.definition.Sns;
import social.hunt.common.definition.SourceType;
import social.hunt.crawler.domain.FeedInfo;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.json.JsonUtil;
import com.sa.common.util.StringUtils;
import com.sa.common.util.UrlUtil;
import com.sa.redis.definition.RedisDefinition.ContentDeduplicationDef;
import com.sa.redis.util.RedisUtil;

/**
 * 
 * 
 * @author lewis
 *
 */
public class IndexDedupUtil {

	private static final Logger log = LoggerFactory.getLogger(IndexDedupUtil.class);

	private static final long ALLOW_UPDATE_MS = 1000 * 60 * 60 * 24 * 4;

	/**
	 * 
	 */
	public IndexDedupUtil() {
	}

	public static String generateHexKey(String domain, String title, String content) {
		return DigestUtils.md5Hex(StringUtils.join(new String[] { domain, title, content }));
	}

	public static List<FeedInfo> fuzzyPreIndexForFeedInfo(List<FeedInfo> feedInfos) throws Exception {
		if (feedInfos == null || feedInfos.isEmpty())
			return ListUtils.EMPTY_LIST;

		List<String> keys = new ArrayList<String>();

		for (FeedInfo feedInfo : feedInfos) {
			String domain = getDomain(feedInfo);
			keys.add(getKey(domain, generateHash(feedInfo)));
		}

		List<String> results = RedisUtil.getIndexDedupInstance().mget(keys.toArray(new String[] {}));

		if (feedInfos.size() == results.size()) {
			List<FeedInfo> outList = new ArrayList<FeedInfo>();
			for (int i = 0; i < feedInfos.size(); i++) {
				if (StringUtils.isBlank(results.get(i))) {
					outList.add(feedInfos.get(i));
				}
			}

			log.info("Fuzzy Deduplication: inList:{}, outList:{}", feedInfos.size(), outList.size());
			return outList;
		} else {
			return feedInfos;
		}
	}

	/**
	 * Pre-Index<BR>
	 * Filter out already indexed or non-updatable records.
	 * 
	 * @param docs
	 * @return
	 */
	public static List<SolrInputDocument> preIndex(List<SolrInputDocument> docs) {
		List<SolrInputDocument> outDocs = new ArrayList<SolrInputDocument>();

		if (docs != null && !docs.isEmpty()) {
			List<String> records = getRecords(docs.toArray(new SolrInputDocument[] {}));
			final long now = System.currentTimeMillis();
			final long days30 = DateUtils.MILLIS_PER_DAY * 30;
			if (docs.size() == records.size()) {

				nextDoc: for (int i = 0; i < docs.size(); i++) {
					SolrInputDocument doc = docs.get(i);
					String value = records.get(i);

					SourceType sourceType = SourceType.getSourceTypeById((int) doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE
							.getName()));

					// Avoid inserting SNS doc into Others collection
					if (SourceType.OTHERS.equals(sourceType)) {
						if (doc.containsKey(SolrFieldDefinition.DOMAIN.getName())) {
							String domain = (String) doc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());
							if (!StringUtils.isBlank(domain)) {
								for (Sns sns : Sns.values()) {
									if (sns.getDomain().equals(domain)) {
										continue nextDoc;
									}
								}
							}
						}
					}

					if (StringUtils.isBlank(value)) {
						outDocs.add(doc);
					} else if (StringUtils.isNumeric(value)) {
						switch (sourceType) {
						case SNS:
							if ((now - Long.parseLong(value)) > ALLOW_UPDATE_MS) {
								// Filter out if doc. is more than 30 days old
								Date pDate = (Date) doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
								if (pDate != null && (now - pDate.getTime()) < days30) {
									outDocs.add(doc);
								}
							}
							break;
						default:
							// For other type of document, prefer never update.
						}
					}
				}
			} else {
				log.error("Size not match!");
			}

			log.info("Pre-Index: original doc size: {}, return doc size: {}", docs.size(), outDocs.size());
		}

		return outDocs;
	}

	public static void addRecords(SolrInputDocument... docs) {
		if (docs != null) {
			List<Pair<String, String>> records = new ArrayList<Pair<String, String>>();

			for (SolrInputDocument doc : docs) {
				String domain = getDomain(doc);
				List<String> hexList = generateHashFromDoc(doc);

				for (String hex : hexList) {
					if (!StringUtils.isBlank(domain) && !StringUtils.isBlank(hex)) {
						records.add(Pair.of(domain, hex));
					}
				}
			}

			if (!records.isEmpty()) {
				addRecords(records);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static List<String> getRecords(SolrInputDocument... docs) {
		if (docs != null && docs.length > 0) {
			List<String> keys = new ArrayList<String>();

			for (SolrInputDocument doc : docs) {
				if (doc != null) {
					String domain = getDomain(doc);

					List<String> hashList = generateHashFromDoc(doc);
					if (!hashList.isEmpty()) {
						String hex = hashList.get(0);
						keys.add(getKey(domain, hex));
					}
				}
			}

			if (!keys.isEmpty()) {
				try {
					return RedisUtil.getIndexDedupInstance().mget(keys.toArray(new String[] {}));
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
			}
		}

		return ListUtils.EMPTY_LIST;
	}

	private static void addRecords(List<Pair<String, String>> input) {
		if (input != null && !input.isEmpty()) {
			List<String> keyValues = new ArrayList<String>();
			String nowMs = Long.toString(System.currentTimeMillis());

			for (Pair<String, String> pair : input) {
				String key = getKey(pair.getLeft(), pair.getRight());
				keyValues.add(key);
				keyValues.add(nowMs);
			}

			try {
				RedisUtil.getIndexDedupInstance().mset(keyValues.toArray(new String[] {}));
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	private static String getDomain(SolrInputDocument doc) {
		if (doc.containsKey(DOMAIN.getName())) {
			return StringUtils.defaultString(doc.getFieldValue(DOMAIN.getName()).toString());
		} else {
			final String url = (String) doc.getFieldValue(URL.getName());
			return StringUtils.defaultString(UrlUtil.extractDomain(url));
		}
	}

	private static String getDomain(FeedInfo feedInfo) throws JsonParseException, JsonMappingException, IOException {
		final String url = feedInfo.get(SolrFieldDefinition.URL.getName());
		if (StringUtils.isBlank(url))
			return "";
		return StringUtils.defaultString(UrlUtil.extractDomain(JsonUtil.getMapper().readValue(url, String.class)));
	}

	private static String getKey(String domain, String hex) {
		return new StringBuilder(ContentDeduplicationDef.DEDUP_PREFIX).append(domain).append(hex).toString();
	}

	protected static String generateHash(FeedInfo feedInfo) throws JsonParseException, JsonMappingException, IOException {
		final ObjectMapper mapper = JsonUtil.getMapper();
		String url = null, title = null, content = null;

		String tmp = feedInfo.get(SolrFieldDefinition.URL.getName());
		if (!StringUtils.isBlank(tmp))
			url = mapper.readValue(tmp, String.class);

		tmp = feedInfo.get(SolrFieldDefinition.TITLE.getName());
		if (!StringUtils.isBlank(tmp))
			title = mapper.readValue(tmp, String.class);

		tmp = feedInfo.get(SolrFieldDefinition.CONTENT.getName());
		if (!StringUtils.isBlank(tmp))
			content = mapper.readValue(tmp, String.class);

		return generateHash(url, title, content);
	}

	protected static String generateHash(String url, String title, String content) {
		// Set hash(domain_title_content).

		String domain = StringUtils.getRealDomain(url, true);

		/**
		 * Using md5 hex instead of hashCode, so that a site-wise deduplication is more reliable when needed.
		 */
		return IndexDedupUtil.generateHexKey(domain, title, content);
	}

	protected static List<String> generateHashFromDoc(SolrInputDocument solrDoc) {
		// Set hash(domain_title_content).
		Object value = solrDoc.getFieldValue(SolrFieldDefinition.URL.getName());
		List<String> list = new ArrayList<String>();

		if (null != value && value instanceof String) {
			String url = (String) value;
			String domain = StringUtils.getRealDomain(url, true);

			String title = null;
			value = solrDoc.getFieldValue(SolrFieldDefinition.TITLE.getName());
			if (null != value && value instanceof String) {
				title = (String) value;
			}

			String content = null;
			value = solrDoc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
			
			if(null == value){
				value = title;
			}
			
			if(null == value){
				value = " ";
			}
			
			if (null != value) {
				/**
				 * Using md5 hex instead of hashCode, so that a site-wise deduplication is more reliable when needed.
				 */

				if (value instanceof String) {
					content = (String) value;
					list.add(IndexDedupUtil.generateHexKey(domain, title, content));
				} else if (value instanceof Collection) {
					@SuppressWarnings("unchecked")
					Collection<String> coll = (Collection<String>) value;
					for (String str : coll) {
						content = str;
						list.add(IndexDedupUtil.generateHexKey(domain, title, content));
					}
				}
			}
		}

		return list;
	}

}
