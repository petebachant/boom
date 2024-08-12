use mongodb::bson::doc;

use crate::spatial;
use crate::types;

pub async fn process_alert(
    avro_bytes: Vec<u8>,
    xmatch_configs: &Vec<types::CatalogXmatchConfig>,
    db: &mongodb::Database
) -> Result<Option<i64>, Box<dyn std::error::Error>> {

    // decode the alert
    let alert = match types::Alert::from_avro_bytes(avro_bytes) {
        Ok(alert) => alert,
        Err(e) => {
            println!("Error reading alert packet: {}", e);
            return Ok(None);
        }
    };

    // check if the alert already exists in the alerts collection
    let collection_alert = db.collection("alerts");
    if !collection_alert.find_one(doc! { "candid": &alert.candid }).await.unwrap().is_none() {
        // we return early if there is already an alert with the same candid
        println!("alert with candid {} already exists", &alert.candid);
        return Ok(None);
    }

    // get the object_id, candid, ra, dec from the alert
    let object_id = alert.object_id.clone();
    let candid = alert.candid;
    let ra = alert.candidate.ra;
    let dec = alert.candidate.dec;

    // separate the alert and its history components
    let (
        alert_no_history,
        prv_candidates,
        fp_hist
    ) = alert.pop_history();
    
    // insert the alert into the alerts collection
    let alert_doc = alert_no_history.mongify();
    collection_alert.insert_one(alert_doc).await.unwrap();

    // - new objects - new entry with prv_candidates, fp_hists, xmatches
    // - existing objects - update prv_candidates, fp_hists
    let collection_alert_aux: mongodb::Collection<mongodb::bson::Document> = db.collection("alerts_aux");

    let prv_candidates_doc = prv_candidates.unwrap_or(vec![]).into_iter().map(|x| x.mongify()).collect::<Vec<_>>();
    let fp_hist_doc = fp_hist.unwrap_or(vec![]).into_iter().map(|x| x.mongify()).collect::<Vec<_>>();

    if collection_alert_aux.find_one(doc! { "_id": &object_id }).await.unwrap().is_none() {
        let mut doc = doc! {
            "_id": &object_id,
            "prv_candidates": prv_candidates_doc,
            "fp_hists": fp_hist_doc,
        };
        doc.insert("cross_matches", spatial::xmatch(ra, dec, xmatch_configs, &db).await);

        collection_alert_aux.insert_one(doc).await.unwrap();
    } else {
        let update_doc = doc! {
            "$addToSet": {
                "prv_candidates": { "$each": prv_candidates_doc },
                "fp_hists": { "$each": fp_hist_doc }
            }
        };

        collection_alert_aux.update_one(doc! { "_id": &object_id }, update_doc).await.unwrap();
    }

    Ok(Some(candid))
}