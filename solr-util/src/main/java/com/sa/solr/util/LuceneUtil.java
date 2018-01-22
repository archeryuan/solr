package com.sa.solr.util;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Version;
import org.apache.lucene.index.IndexWriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.sa.solr.definition.SolrField;

public class LuceneUtil {

	private Directory directory;
	private IndexWriter writer;

	private QueryParser parser;

	private int rows;

	private static final Logger log = LoggerFactory.getLogger(LuceneUtil.class);

	public LuceneUtil(int rows) throws IOException {
		init(rows);
		parser = LuceneQueryParserFactory.getInstance().buildDefaultQueryParser();
	}

	public LuceneUtil(int rows, QueryParser parser) throws IOException {
		init(rows);
		this.parser = parser;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public void save(Collection<Document> docs) {
		try {
			writer.addDocuments(docs);
			writer.commit();

		} catch (Exception e) {
			log.error("Error in save", e);
		}
	}

	public void save(Document doc) {
		try {
			writer.addDocument(doc);
			writer.commit();

		} catch (Exception e) {
			log.error("Error in save", e);
		}
	}

	public void deleteByQuery(String where) {
		try {
			Query query = parser.parse(where);
			try {
				writer.deleteDocuments(query);
				writer.commit();
			} catch (IOException e) {
				log.error("Error in delete", e);
			}
		} catch (ParseException e) {
			log.error("Error in parsing delete query", e);
		}
	}

	public void deleteAll() {
		try {
			writer.deleteAll();
			writer.commit();
		} catch (IOException e) {
			log.error("Error in delete", e);
		}
	}

	public List<String> queryAndReturnId(String where) throws Exception {
		return queryAndReturnId(parser.parse(where));
	}

	public List<Document> queryAndReturnAllDocs() throws Exception {
		return queryAndReturnDocs(new MatchAllDocsQuery());
	}

	public int getNumDocs() throws Exception {
		IndexReader reader = DirectoryReader.open(directory);
		return reader.numDocs();
	}

	public void close() {
		try {
			directory.close();
			writer.close();
		} catch (IOException e) {
			log.error("Error in close", e);
		}
	}

	private void init(int rows) throws IOException {
		directory = new RAMDirectory();
		writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_41, new IKAnalyzer()));
		writer.commit();
		this.rows = rows;
	}

	private List<String> queryAndReturnId(Query query) throws Exception {
		IndexReader reader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs topDocs = searcher.search(query, rows);
		List<String> resultList = new ArrayList<String>();

		for (ScoreDoc scoredoc : topDocs.scoreDocs) {
			// Retrieve the matched document and show relevant details
			Document doc = searcher.doc(scoredoc.doc);
			resultList.add(doc.get(SolrField.id.getValue()));
		}
		return resultList;
	}

	public List<Document> queryAndReturnDocs(Query query) throws Exception {
		IndexReader reader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs topDocs = searcher.search(query, rows);
		List<Document> resultDocs = new ArrayList<Document>();

		for (ScoreDoc scoredoc : topDocs.scoreDocs) {
			// Retrieve the matched document and show relevant details
			Document doc = searcher.doc(scoredoc.doc);
			resultDocs.add(doc);
		}
		return resultDocs;
	}
}