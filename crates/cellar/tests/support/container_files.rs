// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! In-memory archive protocol peer; actual daemon behavior is separately tested.
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use http_body_util::Full;
use hyper::{body::Bytes, Response};
use serde_json::json;
use std::{
    collections::HashMap,
    io::{Cursor, Read},
    sync::Mutex,
};

#[derive(Clone)]
struct File {
    data: Vec<u8>,
    mode: u32,
}

#[derive(Default)]
pub(crate) struct Files(Mutex<HashMap<(String, String), File>>);

impl Files {
    pub(super) fn remove(&self, container: &str) {
        self.0
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != container);
    }

    pub(super) fn answer(
        &self,
        method: &str,
        route: &str,
        query: &str,
        bytes: &[u8],
    ) -> Result<Response<Full<Bytes>>> {
        let container = route
            .strip_prefix("/containers/")
            .unwrap()
            .strip_suffix("/archive")
            .unwrap();
        let query = url::form_urlencoded::parse(query.as_bytes()).collect::<HashMap<_, _>>();
        let path = query.get("path").context("archive path")?;
        let mut files = self.0.lock().unwrap();
        if method == "PUT" {
            ensure!(query
                .get("noOverwriteDirNonDir")
                .is_some_and(|value| value == "true"));
            let mut archive = tar::Archive::new(Cursor::new(bytes));
            let mut entries = archive.entries()?;
            let mut entry = entries.next().context("write entry")??;
            ensure!(entry.header().entry_type().is_file());
            let name = entry.path()?.to_str().context("UTF-8 filename")?.to_owned();
            ensure!(!name.contains('/') && name != "." && name != "..");
            let mode = entry.header().mode()?;
            let mut data = Vec::new();
            entry.read_to_end(&mut data)?;
            ensure!(entries.next().is_none());
            let full = format!("{}/{name}", path.trim_end_matches('/'));
            files.insert((container.into(), full), File { data, mode });
            return Ok(Response::builder().body(Full::new(Bytes::new()))?);
        }
        let key = (container.to_owned(), path.to_string());
        if method == "HEAD" {
            let Some(file) = files.get(&key) else {
                return Ok(Response::builder()
                    .status(404)
                    .body(Full::new(Bytes::new()))?);
            };
            let stat = json!({"name":path.rsplit('/').next().unwrap(),"size":file.data.len(),"mode":file.mode,"mtime":"2026-01-01T00:00:00Z","linkTarget":""});
            return Ok(Response::builder()
                .header(
                    "x-docker-container-path-stat",
                    STANDARD.encode(serde_json::to_vec(&stat)?),
                )
                .body(Full::new(Bytes::new()))?);
        }
        ensure!(method == "GET");
        let entries = if let Some(parent) = path.strip_suffix("/.") {
            let prefix = format!("{parent}/");
            files
                .iter()
                .filter_map(|((owner, path), file)| {
                    if owner != container {
                        return None;
                    }
                    path.strip_prefix(&prefix)
                        .map(|name| (name.to_owned(), file.clone()))
                })
                .collect::<Vec<_>>()
        } else if let Some(file) = files.get(&key) {
            vec![(path.rsplit('/').next().unwrap().to_owned(), file.clone())]
        } else {
            return Ok(Response::builder()
                .status(404)
                .body(Full::new(Bytes::new()))?);
        };
        let mut archive = tar::Builder::new(Vec::new());
        for (name, file) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(file.data.len() as u64);
            header.set_mode(file.mode);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(1);
            header.set_cksum();
            archive.append_data(&mut header, name, file.data.as_slice())?;
        }
        Ok(Response::builder()
            .header("content-type", "application/x-tar")
            .body(Full::new(Bytes::from(archive.into_inner()?)))?)
    }
}
