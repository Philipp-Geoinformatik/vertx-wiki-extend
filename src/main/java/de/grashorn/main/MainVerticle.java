package de.grashorn.main;

import de.grashorn.persistence.ExternalDatabaseVerticle;
import de.grashorn.persistence.InternalDatabaseVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class MainVerticle extends AbstractVerticle {
	@Override
	public void start(Future<Void> startFuture) throws Exception {
		Future<String> dbVerticleDeployment = Future.future();
		vertx.deployVerticle(new InternalDatabaseVerticle(), dbVerticleDeployment.completer());
		dbVerticleDeployment.compose(id -> {
			Future<String> httpVerticleDeployment = Future.future();
			vertx.deployVerticle("de.grashorn.httpserver.HttpServerVerticle", new DeploymentOptions().setInstances(2),
					httpVerticleDeployment.completer());
			return httpVerticleDeployment;
		}).setHandler(ar -> {
			if (ar.succeeded()) {
				startFuture.complete();
			} else {
				startFuture.fail(ar.cause());
			}
		});
	}

	public static void main(String[] args) {
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(new MainVerticle());
	}
}
