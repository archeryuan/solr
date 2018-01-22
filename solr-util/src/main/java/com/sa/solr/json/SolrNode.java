/**
 * 
 */
package com.sa.solr.json;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author lewis
 * 
 */
public class SolrNode {
	private String shard;
	private String roles;
	private String state;
	private String core;
	private String collection;
	private String nodeName;
	private String baseUrl;
	private Boolean leader;

	/**
	 * 
	 */
	public SolrNode() {
	}

	/**
	 * @return the shard
	 */
	@JsonProperty("shard")
	public String getShard() {
		return shard;
	}

	/**
	 * @param shard
	 *            the shard to set
	 */
	@JsonProperty("shard")
	public void setShard(String shard) {
		this.shard = shard;
	}

	/**
	 * @return the roles
	 */
	@JsonProperty("roles")
	public String getRoles() {
		return roles;
	}

	/**
	 * @param roles
	 *            the roles to set
	 */
	@JsonProperty("roles")
	public void setRoles(String roles) {
		this.roles = roles;
	}

	/**
	 * @return the state
	 */
	@JsonProperty("state")
	public String getState() {
		return state;
	}

	/**
	 * @param state
	 *            the state to set
	 */
	@JsonProperty("state")
	public void setState(String state) {
		this.state = state;
	}

	/**
	 * @return the core
	 */
	@JsonProperty("core")
	public String getCore() {
		return core;
	}

	/**
	 * @param core
	 *            the core to set
	 */
	@JsonProperty("core")
	public void setCore(String core) {
		this.core = core;
	}

	/**
	 * @return the collection
	 */
	@JsonProperty("collection")
	public String getCollection() {
		return collection;
	}

	/**
	 * @param collection
	 *            the collection to set
	 */
	@JsonProperty("collection")
	public void setCollection(String collection) {
		this.collection = collection;
	}

	/**
	 * @return the nodeName
	 */
	@JsonProperty("node_name")
	public String getNodeName() {
		return nodeName;
	}

	/**
	 * @param nodeName
	 *            the nodeName to set
	 */
	@JsonProperty("node_name")
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	/**
	 * @return the baseUrl
	 */
	@JsonProperty("base_url")
	public String getBaseUrl() {
		return baseUrl;
	}

	/**
	 * @param baseUrl
	 *            the baseUrl to set
	 */
	@JsonProperty("base_url")
	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	/**
	 * @return the leader
	 */
	@JsonProperty("leader")
	public Boolean isLeader() {
		if (leader == null) {
			leader = false;
		}
		return leader;
	}

	/**
	 * @param leader
	 *            the leader to set
	 */
	@JsonProperty("leader")
	public void setLeader(Boolean leader) {
		this.leader = leader;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SolrNode [");
		if (shard != null) {
			builder.append("shard=");
			builder.append(shard);
			builder.append(", ");
		}
		if (roles != null) {
			builder.append("roles=");
			builder.append(roles);
			builder.append(", ");
		}
		if (state != null) {
			builder.append("state=");
			builder.append(state);
			builder.append(", ");
		}
		if (core != null) {
			builder.append("core=");
			builder.append(core);
			builder.append(", ");
		}
		if (collection != null) {
			builder.append("collection=");
			builder.append(collection);
			builder.append(", ");
		}
		if (nodeName != null) {
			builder.append("nodeName=");
			builder.append(nodeName);
			builder.append(", ");
		}
		if (baseUrl != null) {
			builder.append("baseUrl=");
			builder.append(baseUrl);
			builder.append(", ");
		}
		if (leader != null) {
			builder.append("leader=");
			builder.append(leader);
		}
		builder.append("]");
		return builder.toString();
	}

}
