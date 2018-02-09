package de.grashorn.persistence;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ExternalDatabaseVerticle extends AbstractVerticle {
	private static final Logger LOGGER = LoggerFactory.getLogger(InternalDatabaseVerticle.class);

	public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
	public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
	public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
	public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";

	public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

	private enum SqlQuery {
		CREATE_PAGES_TABLE, ALL_PAGES, GET_PAGE, CREATE_PAGE, SAVE_PAGE, DELETE_PAGE
	}

	private final HashMap<SqlQuery, String> sqlQueries = new HashMap<>();
	
	private void loadSqlQueries() throws IOException {
		String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
		InputStream queriesInputStream;
		if (queriesFile != null) {
			queriesInputStream = new FileInputStream(queriesFile);
		} else {
			queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
		}
		Properties queriesProps = new Properties();
		queriesProps.load(queriesInputStream);
		queriesInputStream.close();
		sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"));
		sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"));
		sqlQueries.put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"));
		sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"));
		sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"));
		sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"));
	}
	// To create a PostgreSQL client:
	JsonObject postgreSQLClientConfig;
	SQLClient postgreSQLClient;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		loadSqlQueries();
		connect(startFuture);// connecting to the postgresql database
	}
	
	private void connect(Future<Void> startFuture) {
		postgreSQLClientConfig = new JsonObject().put("host", "localhost").put("port", 5432).put("username", "postgres")
				.put("password", "admin123").put("database", "livewithme").put("queryTimeout", 2000);
		postgreSQLClient = PostgreSQLClient.createShared(vertx, postgreSQLClientConfig, "PostgreSQLPool1");
		postgreSQLClient.getConnection(ar -> {
			if (ar.failed()) {
				LOGGER.error("Could not open a database connection", ar.cause());
				startFuture.fail(ar.cause());
			} else {
				SQLConnection connection = ar.result();
				connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
					connection.close();
					if (create.failed()) {
						LOGGER.error("Database preparation error", create.cause());
						startFuture.fail(create.cause());
					} else {
						vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE,"wikidb.queue"), this::onMessage);
						startFuture.complete();
					}
				});
			}
		});
	}

	public enum ErrorCodes {
		NO_ACTION_SPECIFIED, BAD_ACTION, DB_ERROR
	}

	public void onMessage(Message<JsonObject> message) {
		LOGGER.info("on message");
		if (!message.headers().contains("action")) {
			LOGGER.error("No action header specified for message with headers {} and body {}", message.headers(),
					message.body().encodePrettily());
			message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
			return;
		}
		String action = message.headers().get("action");
		switch (action) {
			case "all-pages" :
				fetchAllPages(message);
				break;
			case "get-page" :
				fetchPage(message);
				break;
			case "create-page" :
				createPage(message);
				break;
			case "save-page" :
				savePage(message);
				break;
			case "delete-page" :
				deletePage(message);
				break;
			default :
				message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action);
		}
	}
	private void fetchAllPages(Message<JsonObject> message) {
		postgreSQLClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
			if (res.succeeded()) {
				List<String> pages = res.result().getResults().stream().map(json -> json.getString(0)).sorted()
						.collect(Collectors.toList());
				message.reply(new JsonObject().put("pages", new JsonArray(pages)));
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}
	private void fetchPage(Message<JsonObject> message) {
		String requestedPage = message.body().getString("page");
		JsonArray params = new JsonArray().add(requestedPage);
		postgreSQLClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), params, fetch -> {
			if (fetch.succeeded()) {
				JsonObject response = new JsonObject();
				ResultSet resultSet = fetch.result();
				if (resultSet.getNumRows() == 0) {
					response.put("found", false);
				} else {
					response.put("found", true);
					JsonArray row = resultSet.getResults().get(0);
					response.put("id", row.getInteger(0));
					response.put("rawContent", row.getString(1));
				}
				message.reply(response);
			} else {
				reportQueryError(message, fetch.cause());
			}
		});
	}
	private void createPage(Message<JsonObject> message) {
		LOGGER.info(this.getClass().getName() + "Create Page");
		JsonObject request = message.body();
		JsonArray data = new JsonArray().add(request.getString("title")).add(request.getString("markdown"));
		postgreSQLClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
			if (res.succeeded()) {
				message.reply("ok");

			} else {
				reportQueryError(message, res.cause());
			}
		});
	}
	private void savePage(Message<JsonObject> message) {
		LOGGER.info(this.getClass().getName() + "Save Page");
		JsonObject request = message.body();
		JsonArray data = new JsonArray().add(request.getString("markdown")).add(request.getString("id"));
		postgreSQLClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
			if (res.succeeded()) {
				message.reply("ok");
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}
	private void deletePage(Message<JsonObject> message) {

		JsonArray data = new JsonArray().add(message.body().getString("id"));
		postgreSQLClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
			if (res.succeeded()) {
				message.reply("ok");
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}
	
	private void reportQueryError(Message<JsonObject> message, Throwable cause) {
		LOGGER.error("Database query error", cause);
		message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
	}

	private File getFile(String fileName) {
		ClassLoader classLoader = getClass().getClassLoader();
		String pathToFile = classLoader.getResource(fileName).getPath();
		return new File(pathToFile);
	}

	// public static void main(String[] args) {
	// Vertx.vertx().deployVerticle(new ExternalDatabaseVerticle());;
	// }

}
