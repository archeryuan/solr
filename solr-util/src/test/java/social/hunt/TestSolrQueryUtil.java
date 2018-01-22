package social.hunt;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.sa.common.definition.SolrFieldDefinition;

public class TestSolrQueryUtil {
	private static final Logger log = LoggerFactory.getLogger(TestSolrQueryUtil.class);

	private SolrQueryUtil solrQueryUtil = new SolrQueryUtil();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	// // @Test
	// public void testQueryOr() {
	// Set<String> orKeywords = new HashSet<String>();
	// orKeywords.add("周大福");
	// orKeywords.add("股市");
	//
	// Set<String> excludeKeywords = new HashSet<String>();
	// excludeKeywords.add("不好");
	// excludeKeywords.add("下跌");
	//
	// Calendar cal = Calendar.getInstance();
	// Date dateEnd = cal.getTime();
	//
	// cal.add(Calendar.DATE, -30);
	// Date dateStart = cal.getTime();
	//
	// String[] collections = { SolrCollection.DISCUSSION.getValue() };
	// SolrDocumentList docs;
	// try {
	// docs = solrQueryUtil.queryOr(null, null, null, orKeywords, null, dateStart, dateEnd, excludeKeywords, SolrFieldDefinition.values(),
	// 4, 10,
	// collections);
	// log.info("size: {}, docs: {}", docs.size(), docs);
	// } catch (Exception e) {
	// log.error(e.getMessage(), e);
	// }
	//
	// }
	//
	// //@Test
	// public void testQuery() {
	// Set<String> orKeywords = new HashSet<String>();
	// orKeywords.add("周大福");
	// orKeywords.add("股市");
	//
	// Set<String> excludeKeywords = new HashSet<String>();
	// // excludeKeywords.add("不好");
	// // excludeKeywords.add("下跌");
	//
	// Calendar cal = Calendar.getInstance();
	// Date dateEnd = cal.getTime();
	//
	// cal.add(Calendar.DATE, -30);
	// Date dateStart = cal.getTime();
	//
	// String[] collections = { SolrCollection.DISCUSSION.getValue() };
	// Pair<Long, List<Feed>> result = null;
	// try {
	// result = solrQueryUtil.query(null, orKeywords, null, null, null, null, null, null, excludeKeywords, SolrFieldDefinition.PUBLISH_DATE,
	// ORDER.desc, SolrFieldDefinition.values(), 0, 2, null, null, 1, 200, collections);
	// } catch (Exception e) {
	// log.error(e.getMessage(), e);
	// }
	//
	// log.info("Result: {}, {}", result.getLeft(), result.getRight());
	//
	// }
	//
	// @Test
	// public void testSolrQuery() throws SolrServerException, IOException {
	// final int ROWS = 2;
	// SolrQueryUtil solrUtil = new SolrQueryUtil();
	// String keyword = "周大福";
	// String cursorMark = "AoE/GGh0dHA6Ly9hcnQuY2hpbmEuY24vdHNsei8yMDE0LTEyLzA1L2NvbnRlbnRfNzQyMTY0My5odG0=";
	//
	// SolrQuery solrQuery = new SolrQuery();
	//
	// StringBuilder query = new StringBuilder();
	// query.append("+(")
	// .append(StringUtils.join(new String[] { StringUtils.doubleQuote(ChineseTranslator.getInstance().simpl2Trad(keyword)),
	// StringUtils.doubleQuote(ChineseTranslator.getInstance().trad2Simpl(keyword)) }, " ")).append(") ");
	//
	// log.debug("Query string: {}", query.toString());
	// solrQuery.setQuery(query.toString());
	//
	// solrQuery.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
	// //solrQuery.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
	//
	// // int page = 0;
	// // solrQuery.setStart(page * ROWS);
	// solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
	// solrQuery.setRows(ROWS);
	//
	// QueryResponse response = solrUtil.queryResponse(solrQuery, SolrCollection.NEWS.getValue());
	//
	// SolrDocumentList solrDocs = null;
	// String nextCursorMark = response.getNextCursorMark();
	// log.info("nextCursorMark: {}", nextCursorMark);
	// if (!cursorMark.equals(nextCursorMark)) {
	// solrDocs = response.getResults();
	// cursorMark = nextCursorMark;
	// }
	//
	// if (null == solrDocs) {
	// log.error("No doc found");
	// return;
	// }
	//
	// log.info("size: {}", solrDocs.size());
	// for (int i = 0; i < solrDocs.size(); ++i) {
	// log.info("doc {} title: {}", (i + 1), solrDocs.get(i).getFieldValue("title"));
	// }
	// }
	//
	// @Test
	// public void testTranslator() {
	// Set<String> orKeywords = new HashSet<String>();
	// orKeywords.add("周大福");
	// orKeywords.add("大陆");
	//
	// log.info("Result: {}", SolrQueryUtil.toOrQuery(orKeywords));
	//
	// }

	// @Test
	public void testFacets() {
		SolrQueryUtil util = new SolrQueryUtil();
		SolrQuery solrQ = new SolrQuery();
		solrQ.setQuery("+level: 1");
		// solrQ.addFacetField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
		// solrQ.addFacetField(SolrFieldDefinition.SNS_TYPE.getName());
		// solrQ.addFacetField(SolrFieldDefinition.Region.getName());
		solrQ.addFacetField(SolrFieldDefinition.DOMAIN.getName());
		solrQ.setFacetLimit(20);
		solrQ.setFacetMinCount(1);
		log.debug("Solr Query: {}", solrQ.toString());

		QueryResponse response = null;

		try {
			response = util.queryResponse(solrQ, SolrCollection.getCollectionString(SolrCollection.OTHERS));
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<FacetField> results = response.getFacetFields();
		log.info("size: {}", results.size());
		for (FacetField result : results) {
			log.debug("Field: {}", result.getName());
			for (Count count : result.getValues()) {
				// log.info("field: {} value: {}", result.getName(), count.getName());

			}
		}
	}

	@Test
	public void test() {

	}
}
