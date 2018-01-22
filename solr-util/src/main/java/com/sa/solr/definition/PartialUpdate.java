package com.sa.solr.definition;

public enum PartialUpdate {
       Set("set"),
       Add("add"),
       Inc("inc");
       private String value;
       
       
       
       public String getValue() {
		return value;
	}



	public void setValue(String value) {
		this.value = value;
	}



	private PartialUpdate(String value){
    	   this.value = value;
       }
}
