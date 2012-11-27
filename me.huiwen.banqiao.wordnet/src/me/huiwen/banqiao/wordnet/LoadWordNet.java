package me.huiwen.banqiao.wordnet;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;
import java.util.ArrayList;

import info.aduna.iteration.CloseableIteration;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.oupls.sail.*;


import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;

public class LoadWordNet {

	public static void main(String[] argv) throws  IOException,
			 SailException, RDFParseException, RDFHandlerException {
		String fileName = "C:\\software\\NLP\\Wordnet_rdf\\wordnet-glossary.ttl";
		String url = "@prefix wn30: <http://purl.org/vocabularies/princeton/wn30/> .";
		Neo4jGraph graph = new Neo4jGraph("/tmp/neo4j/wordnet");
		
		//GraphDatabaseService ds = new RestGraphDatabase("http://localhost:7474/db/data");
		//Neo4jGraph graph = new Neo4jGraph(ds);
		
		graph.setCheckElementsInTransaction(true);
		Sail sail = new GraphSail<Neo4jGraph>(graph);
		sail.initialize();
		SailConnection sc = sail.getConnection();
		ValueFactory valueFactory = sail.getValueFactory();
		ArrayList<Statement> myList = new ArrayList<Statement>();
		//manager = TransactionalGraphHelper.createCommitManager(graph, 10000)

		StatementCollector collector = new StatementCollector(myList);
		
		TurtleParser parser = new TurtleParser(valueFactory);
		parser.setRDFHandler(collector);
		
		parser.setStopAtFirstError(false);

		File f = new File(fileName);
		FileInputStream fin = new FileInputStream(f);
		BufferedInputStream bin = new BufferedInputStream(fin);
		parser.parse(bin, url);

		for (int i=0;i<myList.size();i++)
		{
			Statement st=myList.get(i);
			sc.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
		}
		
		System.out.println("get statements: ?s ?p ?o ?g");
		
	
		
		CloseableIteration<? extends Statement, SailException> results = sc
				.getStatements(null, null, null, false);
		
		while (results.hasNext()) {
			System.out.println(results.next());
		}

		System.out.println("\nget statements: http://tinkerpop.com#3 ?p ?o ?g");
		results = sc.getStatements(
				valueFactory.createURI("http://purl.org/vocabularies/princeton/wn20/"), null, null,
				false);
		while (results.hasNext()) {
			System.out.println(results.next());
		}

		sc.close();
		graph.shutdown();
		sail.shutDown();
	}
}
