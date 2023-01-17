package org.kpelissier;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;


import java.io.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MovieLensParser implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(MovieLensParser.class.getName());
    private final Driver driver;

    public MovieLensParser(String uri, String user, String password, Config config) {
        // The driver is a long living object and should be opened during the start of your application
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), config);
    }

    @Override
    public void close() {
        // The driver object should be closed before the application ends.
        driver.close();
    }

    public BufferedReader openCSVFromRessource(String path) {
        InputStream is = MovieLensParser.class.getClassLoader().getResourceAsStream(path);
        return new BufferedReader(new InputStreamReader(is));
    }


    public void importMovies() throws IOException {

        System.out.println("Importing rating.csv ...");

        var reader = openCSVFromRessource("ml-latest-small/movies.csv");
        String line = reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] components = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            int movieId = Integer.parseInt(components[0]);
            String title = components[1].replaceAll("\"", "");
            String[] genres = components[2].split("\\|");

            // Create regex to plit movie title and year.
            // String pattern = "(.*)\\s*\\((\\d+)\\)";
            // Pattern r = Pattern.compile(pattern);
            // Matcher m = r.matcher(title);

            // String year = "";
            // if(m.find()) {
            //     title = m.group(1);
            //     year = m.group(2);
            // }

            try(var session = driver.session(SessionConfig.forDatabase("neo4j"))) {

                // Unique constraint query.
                var constraintQuery = new Query(
                        """
                            CREATE CONSTRAINT IF NOT EXISTS
                            FOR (n:Movie)
                            REQUIRE n.movieId IS UNIQUE
                            """);

                // Write transactions allow the driver to handle retries and transient errors
                session.executeWrite(tx -> tx.run(constraintQuery));

                // Create Movie query.
                var movieQuery = new Query(
                        """
                        CREATE (m:Movie { movieId: $movieId, title: $title })
                        """,
                        Map.of("movieId", movieId, "title", title));

                // Write transactions allow the driver to handle retries and transient errors
                session.executeWrite(tx -> tx.run(movieQuery));

                // Create and link Genre query.
                for (String genre: genres) {
                    var genreQuery = new Query(
                            """
                            MATCH (m:Movie { movieId: $movieId })
                            MERGE (g:Genre { genr: $genre })
                            CREATE (m)-[:HAS]->(g)
                            """,
                            Map.of("genre", genre, "movieId", movieId));

                    // Write transactions allow the driver to handle retries and transient errors
                    session.executeWrite(tx -> tx.run(genreQuery));
                    // System.out.printf( "Created Genre: %s", genre);
                }
            } catch (Neo4jException ex) {
                LOGGER.log(Level.SEVERE, " Neo4j raised an exception", ex);
                throw ex;
            }
        }
    }

    public void importRatings() throws IOException {

        System.out.println("Importing rating.csv ...");

        var reader = openCSVFromRessource("ml-latest-small/ratings.csv");
        String line = reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] components = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            int uid = Integer.parseInt(components[0]);
            int movieId = Integer.parseInt(components[1].replaceAll("\"", ""));
            int rating = (int)Double.parseDouble(components[2]);
            String timespamp = components[3];

            try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {

                // Create Movie query.
                var movieQuery = new Query(
                        """
                                MATCH (m:Movie { movieId: $movieId })
                                CREATE (u:User { userId: $uid })
                                CREATE (u)-[:RATED { rating: $rating, timesptamp: $timestamp}]->(m)
                                """,
                        Map.of("uid", uid, "movieId", movieId, "rating", rating, "timestamp", timespamp));

                // Write transactions allow the driver to handle retries and transient errors
                session.executeWrite(tx -> tx.run(movieQuery));

            } catch (Neo4jException ex) {
                LOGGER.log(Level.SEVERE, " Neo4j raised an exception", ex);
                throw ex;
            }
        }
    }

    public void importTags() {

    }

    public static void main(String... args) {

        // Connect to local cluster
        var uri = "bolt://localhost:7687";
        var user = "neo4j";
        var password = "123456789";

        try (var app = new MovieLensParser(uri, user, password, Config.defaultConfig())) {
            app.importMovies();
            app.importRatings();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}