/**
 * 
 */
package social.hunt.solr.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.common.definition.Sns;
import social.hunt.solr.definition.SolrCollection;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.definition.SourceType;
import com.sa.common.util.HttpClientUtil;

/**
 * Utility class for migration data from one Solr cluster to another.<BR>
 * Both Solr cluster should have identical collection and schema.
 * 
 * @author lewis
 *
 */
public class DataMigrator {

	private static final Logger log = LoggerFactory.getLogger(DataMigrator.class);

	public static void main(String[] args) throws IOException {
		DataMigrator migrator = new DataMigrator();

		Integer day = null;
		String srcZk = "zk1:2181,zk2:2181,hadoop-node2:2181,hadoop-node1:2181,hadoop-master1:2181/solr5";
		String destZk = "zk1:2181,zk2:2181,hadoop-node2:2181,hadoop-node1:2181,hadoop-master1:2181/prod-solr5";

		if (args != null && args.length == 2) {
			srcZk = args[0];
			destZk = args[1];
		} else if (args != null && args.length == 3) {
			srcZk = args[0];
			destZk = args[1];
			day = Integer.parseInt(args[2]);
		}

		if (log.isInfoEnabled()) {
			log.info("Source: {}", srcZk);
			log.info("Destination: {}", destZk);
		}

		for (SolrCollection coll : SolrCollection.values()) {
			migrator.migrate(new LoaderConfig(srcZk, coll.getValue()), new LoaderConfig(destZk, coll.getValue()), day);
		}
	}

	public void migrate(LoaderConfig source, LoaderConfig destination, Integer days) throws IOException {

		log.info("source: {}", source);
		log.info("destination: {}", destination);

		String cursorMark = "*";
		QueryResponse rep = null;
		CloudSolrClient srcClient = null, destClient = null;
		SolrQuery solrQ = new SolrQuery();
		if (days == null) {
			solrQ.setQuery("*:*");
		} else {
			solrQ.setQuery(new StringBuilder().append("+cDate:[NOW-").append(days).append("DAY TO NOW] -snsType:(1 2 5 9)").toString());
		}
		solrQ.setRows(4000);
		solrQ.add("collection", source.getCollection());
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		boolean done = false;

		try {
			srcClient = getSolrClient(source);

			while (!done) {
				List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

				solrQ.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
				rep = srcClient.query(solrQ);

				for (SolrDocument doc : rep.getResults()) {
					SolrInputDocument inputDoc = modifyDoc(ClientUtils.toSolrInputDocument(doc));
					if (inputDoc != null)
						docs.add(inputDoc);
				}
				log.info("Prepared {} solr input documents.", docs.size());
				if (!docs.isEmpty()) {
					destClient = getSolrClient(destination);
					destClient.add(destination.getCollection(), docs, 1000 * 60);
					destClient.close();
					destClient = null;
					log.info("Closed destination client.");
				}

				String nextCursorMark = rep.getNextCursorMark();
				if (cursorMark.equals(nextCursorMark))
					done = true;
				cursorMark = nextCursorMark;

				try {

					Thread.sleep(2000L);
				} catch (InterruptedException e) {
					log.error(e.getMessage(), e);
				} finally {
					log.info("Thread woke up.");
				}
			}
		} catch (SolrServerException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} finally {
			if (srcClient != null)
				srcClient.close();
			if (destClient != null)
				destClient.close();
		}

	}

	private SolrInputDocument modifyDoc(SolrInputDocument doc) {
		if (doc != null) {
			// Clear the version history
			doc.removeField("_version_");

			// Old invalid doc.
			if (doc.containsKey("date")) {
				return null;
			}

			if (doc.containsKey(SolrFieldDefinition.SOURCE_TYPE.getName())) {
				int sType = (int) doc.getFieldValue(SolrFieldDefinition.SOURCE_TYPE.getName());
				if (SourceType.SINA.getSourceTypeDBStr() == sType) {
					doc.setField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(),
							(int) social.hunt.common.definition.SourceType.SNS.getSourceTypeId());
					doc.setField(SolrFieldDefinition.SNS_TYPE.getName(), (int) Sns.WEIBO.getSnsId());
				} else if (SourceType.SOUGO_WEIXIN.getSourceTypeDBStr() == sType) {
					doc.setField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(),
							(int) social.hunt.common.definition.SourceType.SNS.getSourceTypeId());
					//doc.setField(SolrFieldDefinition.SNS_TYPE.getName(), (int) Sns.WEIXIN.getSnsId());
				} else if (SourceType.TWITTER.getSourceTypeDBStr() == sType) {
					return null;
					// doc.addField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(),
					// (int) social.hunt.common.definition.SourceType.SNS.getSourceTypeId());
					// doc.addField(SolrFieldDefinition.SNS_TYPE.getName(), (int) Sns.TWITTER.getSnsId());
				} else if (SourceType.FORUM.getSourceTypeDBStr() == sType) {
					doc.setField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(),
							(int) social.hunt.common.definition.SourceType.FORUM.getSourceTypeId());
				} else if (SourceType.INSTAGRAM.getSourceTypeDBStr() == sType) {
					return null;
				} else if (SourceType.YOUTUBE.getSourceTypeDBStr() == sType) {
					return null;
				}
			}
		}
		return doc;
	}

	private CloudSolrClient getSolrClient(LoaderConfig config) {
		CloudSolrClient client = new CloudSolrClient(config.getZk(), HttpClientUtil.getInstance().getClient());

		// Refer to CloudSolrClient's javadoc.
		// This class assumes the id field for your documents is called 'id' - if this is not the case, you must set the right name with
		// setIdField(String).
		client.setIdField(SolrFieldDefinition.URL.getName());

		return client;
	}

	public static class LoaderConfig {
		private final String zk;
		private final String collection;

		/**
		 * @param zk
		 * @param collection
		 */
		public LoaderConfig(String zk, String collection) {
			super();
			this.zk = zk;
			this.collection = collection;
		}

		/**
		 * @return the zk
		 */
		public String getZk() {
			return zk;
		}

		/**
		 * @return the collection
		 */
		public String getCollection() {
			return collection;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("LoaderConfig [zk=");
			builder.append(zk);
			builder.append(", collection=");
			builder.append(collection);
			builder.append("]");
			return builder.toString();
		}

	}
}
