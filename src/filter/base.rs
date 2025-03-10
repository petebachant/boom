use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use tokio::sync::mpsc;
use tracing::error;

use crate::utils::worker::WorkerCmd;

#[derive(thiserror::Error, Debug)]
pub enum FilterError {
    #[error("failed to retrieve filter from database")]
    GetFilterError(#[source] mongodb::error::Error),
    #[error("failed to deserialize filter from database")]
    DeserializeFilterError(#[source] mongodb::error::Error),
    #[error("invalid filter")]
    InvalidFilter(#[source] mongodb::bson::document::ValueAccessError),
    #[error("invalid filter permissions")]
    InvalidFilterPermissions,
    #[error("filter not found in database")]
    FilterNotFound,
    #[error("invalid filter result")]
    InvalidFilterResult(#[source] mongodb::error::Error),
    #[error("failed to run filter")]
    RunFilterError(#[source] mongodb::error::Error),
    #[error("failed to deserialize filter pipeline")]
    DeserializePipelineError(#[source] serde_json::Error),
    #[error("invalid filter pipeline")]
    InvalidFilterPipeline,
    #[error("invalid filter pipeline stage")]
    InvalidFilterPipelineStage(#[source] mongodb::bson::ser::Error),
    #[error("invalid filter id")]
    InvalidFilterId,
}

pub async fn get_filter_object(
    filter_id: i32,
    catalog: &str,
    filter_collection: &mongodb::Collection<mongodb::bson::Document>,
) -> Result<Document, FilterError> {
    let mut filter_obj = filter_collection
        .aggregate(vec![
            doc! {
                "$match": doc! {
                    "filter_id": filter_id,
                    "active": true,
                    "catalog": catalog
                }
            },
            doc! {
                "$project": doc! {
                    "fv": doc! {
                        "$filter": doc! {
                            "input": "$fv",
                            "as": "x",
                            "cond": doc! {
                                "$eq": [
                                    "$$x.fid",
                                    "$active_fid"
                                ]
                            }
                        }
                    },
                    "group_id": 1,
                    "permissions": 1,
                    "catalog": 1
                }
            },
            doc! {
                "$project": doc! {
                    "pipeline": doc! {
                        "$arrayElemAt": [
                            "$fv.pipeline",
                            0
                        ]
                    },
                    "group_id": 1,
                    "permissions": 1,
                    "catalog": 1
                }
            },
        ])
        .await
        .map_err(FilterError::GetFilterError)?;

    let advance = filter_obj
        .advance()
        .await
        .map_err(FilterError::GetFilterError)?;
    let filter_obj = if advance {
        filter_obj
            .deserialize_current()
            .map_err(FilterError::DeserializeFilterError)?
    } else {
        return Err(FilterError::FilterNotFound);
    };

    Ok(filter_obj)
}

pub async fn process_alerts(
    candids: Vec<i64>,
    mut pipeline: Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Document>, FilterError> {
    if candids.len() == 0 {
        return Ok(vec![]);
    }
    if pipeline.len() == 0 {
        panic!("filter pipeline is empty, ensure filter has been built before running");
    }

    // insert candids into filter
    pipeline[0]
        .get_document_mut("$match")
        .map_err(FilterError::InvalidFilter)?
        .insert(
            "_id",
            doc! {
                "$in": candids
            },
        );

    // run filter
    let mut result = alert_collection
        .aggregate(pipeline)
        .await
        .map_err(FilterError::RunFilterError)?;

    let mut out_documents: Vec<Document> = Vec::new();

    while let Some(doc) = result.next().await {
        let doc = doc.map_err(FilterError::InvalidFilterResult)?;
        out_documents.push(doc);
    }

    Ok(out_documents)
}

#[async_trait::async_trait]
pub trait Filter {
    async fn build(
        filter_id: i32,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<Self, FilterError>
    where
        Self: Sized;
}

#[derive(thiserror::Error, Debug)]
pub enum FilterWorkerError {
    #[error("failed to load config")]
    LoadConfigError(#[from] crate::conf::BoomConfigError),
    #[error("failed to connect to redis")]
    ConnectRedisError(#[source] redis::RedisError),
    #[error("filter error")]
    FilterError(#[from] FilterError),
    #[error("failed to retrieve filters from database")]
    GetFiltersError(#[source] mongodb::error::Error),
    #[error("failed to pop from the alert filter queue")]
    PopCandidError(#[source] redis::RedisError),
    #[error("failed to push filter results onto the filter results queue")]
    PushFilterResultsError(#[source] redis::RedisError),
    #[error("failed to serialize filter result to json")]
    SerializeFilterResultError(#[source] serde_json::Error),
    #[error("failed to retrive queue name for filter")]
    GetQueueNameError,
    #[error("failed to get filter by queue")]
    GetFilterByQueueError,
}

#[async_trait::async_trait]
pub trait FilterWorker {
    async fn new(
        id: String,
        receiver: mpsc::Receiver<WorkerCmd>,
        config_path: &str,
    ) -> Result<Self, FilterWorkerError>
    where
        Self: Sized;
    async fn run(&mut self) -> Result<(), FilterWorkerError>;
}

#[tokio::main]
pub async fn run_filter_worker<T: FilterWorker>(
    id: String,
    receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), FilterWorkerError> {
    let mut filter_worker = T::new(id, receiver, config_path).await?;
    filter_worker.run().await?;
    Ok(())
}
