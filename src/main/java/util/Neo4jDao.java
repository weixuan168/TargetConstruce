package util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;

public class Neo4jDao {
    private static final String DB_PATH = "target/neo4jexample";
    private static GraphDatabaseService graphDB;
    private static Neo4jDao neo4jDao = new Neo4jDao();

    private Neo4jDao() {
        clearDB();
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(new File(DB_PATH));

        registerShutdownHook(graphDB);
    }

    public static Neo4jDao getInstance() {
        return neo4jDao;
    }

    public GraphDatabaseService getGraphDB() {
        return graphDB;
    }

    private void clearDB() {
        try {
            FileUtils.deleteRecursively(new File(DB_PATH));
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDB) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDB.shutdown();
            }
        });
    }

    private void shutdown() {
        graphDB.shutdown();
    }
}
