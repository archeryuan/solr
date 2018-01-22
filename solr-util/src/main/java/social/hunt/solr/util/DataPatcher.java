/**
 * 
 */
package social.hunt.solr.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.solr.connection.SolrClientManager;

import com.sa.common.definition.SolrFieldDefinition;

/**
 * @author lewis
 *
 */
public class DataPatcher {

	private static final Logger log = LoggerFactory.getLogger(DataPatcher.class);

	public static void main(String[] args) throws SolrServerException, IOException {
		if (args == null || args.length == 0) {
			System.out.println("input: query collection sourceType region snsType");
		} else {
			while (true) {
				doPatch(args);
			}
		}
	}

	public static void doPatch(String[] args) throws SolrServerException, IOException {
		String query = args[0];
		String collection = args[1];
		Integer sourceType = Integer.parseInt(args[2]);
		Integer region = Integer.parseInt(args[3]);

		Integer snsType = null;
		if (args.length == 5) {
			snsType = Integer.parseInt(args[4]);
		}

		SolrQuery solrQuery = new SolrQuery();
		solrQuery.setRows(10000);
		solrQuery.set("shards.tolerant", true);
		// solrQuery.setQuery("-sourceType:* +sType:" + sType + " +domain:" + domain);
		solrQuery.setQuery(query);
		SolrDocumentList list = new SolrQueryUtil().query(solrQuery, collection);
		List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();

		if (list == null || list.isEmpty()) {
			System.exit(0);
		} else {
			log.info("Record count: {}", list.size());
		}

		for (SolrDocument doc : list) {
			doc.setField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(), sourceType);
			doc.setField(SolrFieldDefinition.Region.getName(), region);
			if (snsType != null)
				doc.setField(SolrFieldDefinition.SNS_TYPE.getName(), snsType);
			inputDocs.add(ClientUtils.toSolrInputDocument(doc));
		}

		CloudSolrClient client = null;
		try {
			client = SolrClientManager.getSolrClient(null);
			client.add(collection, inputDocs, 1000 * 60);

			try {
				log.info("Sleeping....");
				Thread.sleep(1000 * 70);
				log.info("Woke up!");
			} catch (Exception e) {
				Thread.interrupted();
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (client != null)
				client.close();
		}
	}
}
