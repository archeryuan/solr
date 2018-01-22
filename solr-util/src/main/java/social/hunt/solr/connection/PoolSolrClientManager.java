/**
 * 
 */
package social.hunt.solr.connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.solr.definition.SolrServer;

/**
 * @author lewis
 *
 */
public class PoolSolrClientManager extends SolrClientManager {

	private static final Logger log = LoggerFactory.getLogger(PoolSolrClientManager.class);

	private Map<SolrServer, CloudSolrClient> pool;
	private List<PoolingHttpClientConnectionManager> managers;

	/**
	 * 
	 */
	public PoolSolrClientManager() {
		super();

		pool = new HashMap<SolrServer, CloudSolrClient>();
		managers = new ArrayList<PoolingHttpClientConnectionManager>();
	}

	public CloudSolrClient getSolrClient(SolrServer server) {
		if (pool.containsKey(server))
			return pool.get(server);
		CloudSolrClient client = null;
		PoolingHttpClientConnectionManager mgr = new PoolingHttpClientConnectionManager();
		managers.add(mgr);
		switch (server) {
		// case NWD_SOLR:
		// client = getNwdSolrClient(newHttpClient(mgr));
		// break;
		default:
			client = getSolrClient(newHttpClient(mgr));
		}
		pool.put(server, client);
		return client;
	}

	protected HttpClient newHttpClient(PoolingHttpClientConnectionManager mgr) {
		return HttpClientBuilder.create().setConnectionManager(mgr)
				.setServiceUnavailableRetryStrategy(new ServiceUnavailableRetryStrategy() {

					@Override
					public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
						if (executionCount > 3)
							return false;
						return true;
					}

					@Override
					public long getRetryInterval() {
						return 1000L;
					}
				}).setRetryHandler(new HttpRequestRetryHandler() {

					@Override
					public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
						if (executionCount > 3)
							return false;
						return true;
					}
				}).build();
	}

	public void shutdown() {
		for (CloudSolrClient client : pool.values()) {
			if (client != null) {
				try {
					client.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
			}
		}
		for (PoolingHttpClientConnectionManager mgr : managers) {
			mgr.shutdown();
		}
	}

}
