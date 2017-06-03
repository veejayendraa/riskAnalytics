package com.vij.riskAnalytics.hierarchy;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Node implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public Node(String node, String businessDate) {
		super();
		this.node = node;
		this.businessDate = businessDate;
	}
	
	@QuerySqlField(orderedGroups={@QuerySqlField.Group(
		    name = "date_hierarchy_idx", order = 1)})
	private String node;
	@QuerySqlField(index = true,orderedGroups={@QuerySqlField.Group(
		    name = "date_hierarchy_idx", order = 0)})
	private String businessDate;

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((businessDate == null) ? 0 : businessDate.hashCode());
		result = prime * result + ((node == null) ? 0 : node.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Node other = (Node) obj;
		if (businessDate == null) {
			if (other.businessDate != null)
				return false;
		} else if (!businessDate.equals(other.businessDate))
			return false;
		if (node == null) {
			if (other.node != null)
				return false;
		} else if (!node.equals(other.node))
			return false;
		return true;
	}
	public String getNode() {
		return node;
	}
	public void setNode(String node) {
		this.node = node;
	}
	public String getBusinessDate() {
		return businessDate;
	}
	public void setBusinessDate(String businessDate) {
		this.businessDate = businessDate;
	}
	
	
}
