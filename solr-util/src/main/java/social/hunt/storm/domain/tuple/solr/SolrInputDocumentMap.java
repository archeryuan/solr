/**
 * 
 */
package social.hunt.storm.domain.tuple.solr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import com.sa.common.definition.SourceType;

/**
 * @author lewis
 *
 */
@SuppressWarnings("deprecation")
public class SolrInputDocumentMap extends HashMap<SourceType, List<SolrInputDocument>> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6002124874263803979L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.HashMap#get(java.lang.Object)
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public List<SolrInputDocument> get(Object key) {
		List<SolrInputDocument> obj = super.get(key);
		if (obj == null) {
			obj = new ArrayList<SolrInputDocument>();
			this.put((SourceType) key, obj);
		}
		return obj;
	}

}
