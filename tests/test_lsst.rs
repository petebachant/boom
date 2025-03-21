use boom::alert::{AlertWorker, LsstAlertWorker};

const CONFIG_FILE: &str = "tests/config.test.yaml";

#[tokio::test]
async fn test_lsst_alert_from_avro_bytes() {
    let mut alert_worker = LsstAlertWorker::new(CONFIG_FILE).await.unwrap();

    let file_name = "tests/data/alerts/lsst/0.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let alert = alert_worker
        .alert_from_avro_bytes(&bytes_content)
        .await
        .unwrap();

    assert_eq!(alert.candid, 25409136044802067);
    assert_eq!(
        alert.candidate.dia_source.object_id.unwrap(),
        25401295582003262
    );

    assert!((alert.candidate.dia_source.ra - 149.802106).abs() < 1e-6);
    assert!((alert.candidate.dia_source.dec - 2.248650).abs() < 1e-6);

    // add mag data to the candidate

    assert!((alert.candidate.dia_source.jd - 2457454.829282).abs() < 1e-6);
    assert!((alert.candidate.magpsf - 23.146893).abs() < 1e-6);
    assert!((alert.candidate.sigmapsf - 0.039097).abs() < 1e-6);
    assert!((alert.candidate.diffmaglim - 25.00841).abs() < 1e-5);
    assert!(alert.candidate.snr - 27.770037 < 1e-6);
    assert_eq!(alert.candidate.isdiffpos, true);
    assert_eq!(alert.candidate.dia_source.band.unwrap(), "g");

    // verify that the prv_candidates are present
    assert!(!alert.prv_candidates.is_none());
    let prv_candidates = alert.prv_candidates.unwrap();
    assert_eq!(prv_candidates.len(), 2);

    // validate the first prv_candidate
    let prv_candidate = prv_candidates.get(0).unwrap();

    assert!((prv_candidate.dia_source.jd - 2457454.7992).abs() < 1e-6);
    assert!((prv_candidate.magpsf - 24.763279).abs() < 1e-6);
    assert!((prv_candidate.sigmapsf - 0.329765).abs() < 1e-6);
    assert!((prv_candidate.diffmaglim - 24.309652).abs() < 1e-6);
    assert!(prv_candidate.snr - 3.292455 < 1e-6);
    assert_eq!(prv_candidate.isdiffpos, true);
    assert_eq!(prv_candidate.dia_source.band.clone().unwrap(), "g");

    // same for the fp_hists
    assert!(!alert.fp_hists.is_none());
    let fp_hists = alert.fp_hists.unwrap();
    assert_eq!(fp_hists.len(), 3);

    // validate the first fp_hist
    let fp_hist = fp_hists.get(0).unwrap();

    assert!((fp_hist.dia_forced_source.jd - 2457454.7992).abs() < 1e-6);
    assert!((fp_hist.magpsf.unwrap() - 24.735056).abs() < 1e-6);
    assert!((fp_hist.sigmapsf.unwrap() - 0.329754).abs() < 1e-6);
    assert!((fp_hist.diffmaglim.unwrap() - 24.281467).abs() < 1e-6);
    assert!((fp_hist.snr.unwrap() - 3.292566).abs() < 1e-6);
    assert_eq!(fp_hist.isdiffpos.unwrap(), true);
    assert_eq!(fp_hist.dia_forced_source.band.clone().unwrap(), "g");

    // validate the non detections
    assert!(!alert.prv_nondetections.is_none());
    // length should be 0
    assert_eq!(alert.prv_nondetections.unwrap().len(), 0);

    // TODO: find an LSST avro packet that has non detections so we can test it
}

#[tokio::test]
async fn test_process_alert() {
    let mut alert_worker = LsstAlertWorker::new(CONFIG_FILE).await.unwrap();

    let file_name = "tests/data/alerts/lsst/0.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let result = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(result, 25409136044802067);
}
