extern crate serenade_optimized;

use sessions::RocksDBSessionStore;

use actix_web::{middleware, http::ContentEncoding, App, HttpServer, web, HttpRequest, HttpResponse};
use actix_web_prom::PrometheusMetrics;

use std::sync::Arc;
use std::time::Duration;
use actix_web::http::header;


use serenade_optimized::sessions as sessions;
use serenade_optimized::endpoints::index_resource::internal;
use serenade_optimized::endpoints::recommend_resource::v1_recommend;
use serenade_optimized::vsknn::offline_index::OfflineIndex;
use serenade_optimized::dataframeutils::SharedHandlesAndConfig;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // The serenade recommender service.
    // By default it uses an index that is computed offline on billions of user-item interactions.
    // It supports training data in a csv file via OfflineIndex::new_from_csv().
    let bind_address = "0.0.0.0:8080";
    let sample_size_m = 500;
    let neighborhood_size_k = 500;

    let training_data_path = std::env::args().nth(1)
        .expect("Training data file not specified!");

    let num_workers: usize = std::env::args().nth(2)
        .expect("Number of actix workers not specified!")
        .parse()
        .expect("Unable to parse number of actix workers");

    // By default we use an index that is computed offline on billions of user-item interactions.
    let vsknn =  Arc::new(OfflineIndex::new(&training_data_path));
    // The following line creates an index directly from a csv file as input.
    // let vsknn =  Arc::new(OfflineIndex::new_from_csv(&training_data_path, sample_size_m));

    println!("start db");
    let session_ttl = Duration::from_secs(30 * 60);
    let db = Arc::new(RocksDBSessionStore::new("./sessions.db", session_ttl));

    println!("start metrics");
    let prometheus = PrometheusMetrics::new("api", Some("/internal/prometheus"), None);

    println!("Done. start httpd at http://{}", bind_address);
    HttpServer::new(move || {

        let handles_and_config = SharedHandlesAndConfig {
            session_store: db.clone(),
            vsknn_index: vsknn.clone(),
            sample_size: sample_size_m,
            neighborhood_size: neighborhood_size_k,
            num_items_to_recommend: 21,
            max_items_in_session: 2,
            qty_workers: num_workers,
            db_compaction_ttl_in_secs: session_ttl.as_secs() as usize,
        };

        App::new()
            .wrap(middleware::Compress::new(ContentEncoding::Identity))
            .wrap(prometheus.clone())
            .wrap(middleware::DefaultHeaders::new()
                .header("Cache-Control", "no-cache, no-store, must-revalidate")
                .header("Pragma", "no-cache")
                .header("Expires", "0")
            )
            .data(handles_and_config)
            .service(v1_recommend)
            .service(internal)
            .service(web::resource("/").route(web::get().to(|_req: HttpRequest| {
                HttpResponse::Found()
                    .header(header::LOCATION, "/internal")
                    .finish()
            })))
    })
        .workers(num_workers)
        .bind(bind_address)
        .unwrap_or_else(|_| panic!("Could not bind server to address {}", &bind_address))
        .run()
        .await

}