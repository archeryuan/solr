package com.sa.solr.domain;

import java.util.List;

public class PartialUpdateDoc {
	
	private String docId;
	private List<PartialUpdateElement> listPartialUpdateE;
	private PartialUpdateElement partialUpdateElement;
	
	public PartialUpdateDoc(String docId,List<PartialUpdateElement> listPartialUpdateE){
		this.docId = docId;
		this.listPartialUpdateE = listPartialUpdateE;
	}
	
	public PartialUpdateDoc(String docId,PartialUpdateElement partialUpdateElement){
		this.docId = docId;
		this.partialUpdateElement = partialUpdateElement;
	}

	public String getDocId() {
		return docId;
	}


	public List<PartialUpdateElement> getListPartialUpdateE() {
		return listPartialUpdateE;
	}

	public PartialUpdateElement getPartialUpdateElement() {
		return partialUpdateElement;
	}

	


}
