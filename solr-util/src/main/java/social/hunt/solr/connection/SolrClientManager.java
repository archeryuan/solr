/**
 * 
 */
package social.hunt.solr.connection;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.HttpClientUtil;
import com.sa.common.util.StringUtils;

/**
 * @author lewis
 *
 */
public class SolrClientManager {

	public static CloudSolrClient getSolrClient(HttpClient httpClient) {
		CloudSolrClient client = null;

		if (httpClient == null) {
			client = new CloudSolrClient(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), HttpClientUtil.getInstance()
					.getClient());
		} else {
			client = new CloudSolrClient(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), httpClient);
		}

		// Refer to CloudSolrClient's javadoc.
		// This class assumes the id field for your documents is called 'id' - if this is not the case, you must set the right name with
		// setIdField(String).
		client.setIdField(SolrFieldDefinition.URL.getName());
		client.setZkClientTimeout(1200000);
		client.setZkConnectTimeout(1200000);

		return client;
	}

	/**
	 * Get Solr client for accessing New World Development Solr Server.<BR>
	 * <b>This method will return null when the server is deprecated.</b>
	 * 
	 * @return
	 */
	public static CloudSolrClient getNwdSolrClient(HttpClient httpClient) {

		// if (StringUtils.isBlank(CommonConfig.getInstance().getDocumentIndexerZookeeper()))
		// return null;
		//
		// String chroot = CommonConfig.getInstance().getDocumentIndexerZookeeperChroot();
		//
		// CloudSolrClient client = null;
		// if (StringUtils.isBlank(chroot)) {
		// client = new CloudSolrClient(CommonConfig.getInstance().getDocumentIndexerZookeeper(), HttpClientUtil.getInstance().getClient());
		// } else {
		// String[] array = StringUtils.split(CommonConfig.getInstance().getDocumentIndexerZookeeper(), ",");
		// client = new CloudSolrClient(Arrays.asList(array), chroot, HttpClientUtil.getInstance().getClient());
		// }

		if (StringUtils.isBlank(CommonConfig.getInstance().getDocumentIndexerZookeeper()))
			return null;

		CloudSolrClient client = null;

		if (httpClient == null) {
			client = new CloudSolrClient(CommonConfig.getInstance().getDocumentIndexerZookeeper(), HttpClientUtil.getInstance().getClient());
		} else {
			client = new CloudSolrClient(CommonConfig.getInstance().getDocumentIndexerZookeeper(), httpClient);
		}

		// Refer to CloudSolrClient's javadoc.
		// This class assumes the id field for your documents is called 'id' - if this is not the case, you must set the right name with
		// setIdField(String).
		client.setIdField(SolrFieldDefinition.URL.getName());

		return client;
	}

}
