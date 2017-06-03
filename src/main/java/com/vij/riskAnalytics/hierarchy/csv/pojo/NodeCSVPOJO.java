package com.vij.riskAnalytics.hierarchy.csv.pojo;

public class NodeCSVPOJO {

	public String getParentNode() {
		return parentNode;
	}
	public void setParentNode(String parentNode) {
		this.parentNode = parentNode;
	}
	public String getChildNode() {
		return childNode;
	}
	public void setChildNode(String childNode) {
		this.childNode = childNode;
	}
	public NodeCSVPOJO(String parentNode, String childNode) {
		super();
		this.parentNode = parentNode;
		this.childNode = childNode;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((childNode == null) ? 0 : childNode.hashCode());
		result = prime * result + ((parentNode == null) ? 0 : parentNode.hashCode());
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
		NodeCSVPOJO other = (NodeCSVPOJO) obj;
		if (childNode == null) {
			if (other.childNode != null)
				return false;
		} else if (!childNode.equals(other.childNode))
			return false;
		if (parentNode == null) {
			if (other.parentNode != null)
				return false;
		} else if (!parentNode.equals(other.parentNode))
			return false;
		return true;
	}
	private String parentNode;
	private String childNode;
}
