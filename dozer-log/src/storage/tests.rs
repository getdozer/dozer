use super::Storage;

pub async fn test_storage_basic<S: Storage>(storage: &S) {
    assert!(storage
        .list_objects(Default::default(), None)
        .await
        .unwrap()
        .objects
        .is_empty());

    let key = "path/to/key".to_string();
    let data = vec![1, 2, 3];
    storage.put_object(key.clone(), data.clone()).await.unwrap();

    let objects = storage
        .list_objects(Default::default(), None)
        .await
        .unwrap()
        .objects;
    assert_eq!(objects[0].key, key);

    let downloaded_data = storage.download_object(key).await.unwrap();
    assert_eq!(downloaded_data, data);
}

pub async fn test_storage_multipart<S: Storage>(storage: &S) {
    let key = "path/to/key".to_string();
    let upload_id = storage.create_multipart_upload(key.clone()).await.unwrap();

    let data = vec![(1, vec![1; 5 * 1024 * 1024]), (2, vec![1, 2, 3])];
    let mut parts = vec![];
    for (part_number, data) in data.iter() {
        let e_tag = storage
            .upload_part(key.clone(), upload_id.clone(), *part_number, data.clone())
            .await
            .unwrap();
        parts.push((*part_number, e_tag));
    }

    storage
        .complete_multipart_upload(key.clone(), upload_id.clone(), parts)
        .await
        .unwrap();

    let downloaded_data = storage.download_object(key).await.unwrap();
    assert_eq!(
        downloaded_data,
        data.into_iter()
            .flat_map(|(_, data)| data)
            .collect::<Vec<_>>()
    );
}

pub async fn test_storage_prefix<S: Storage>(storage: &S) {
    let prefix_key = "prefix/key".to_string();
    let other_key = "other/key".to_string();
    let data = vec![1, 2, 3];
    storage
        .put_object(prefix_key.clone(), data.clone())
        .await
        .unwrap();
    storage
        .put_object(other_key.clone(), data.clone())
        .await
        .unwrap();

    let objects = storage
        .list_objects("prefix".to_string(), None)
        .await
        .unwrap()
        .objects;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, prefix_key);
}
