use crate::{
    config::Config,
    db::{Mod, PublishKey},
    errors::TryExt,
    file_repo::{self, FileRepo},
};
use bytes::Bytes;
use semver::{Version, VersionReq};
use serde::Deserialize;
use sqlx::SqlitePool;
use tokio::fs;
use warp::{
    Filter, Rejection, Reply,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
};

#[inline]
fn one() -> usize {
    1
}

#[inline]
fn any_version() -> VersionReq {
    VersionReq::STAR
}

#[derive(Debug, Deserialize)]
struct ResolveQuery {
    #[serde(default = "any_version")]
    req: VersionReq,
    #[serde(default = "one")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct OptPublishKey {
    pw: Option<String>,
    user: Option<String>,
}

pub fn handler(
    pool: &'static SqlitePool,
    config: &'static Config,
    file_repo: &'static FileRepo,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Send + Sync + Clone + 'static {
    // GET /
    let list = warp::path::end()
        .and(warp::get())
        .and_then(move || list(pool));

    // GET /{package}
    let resolve = warp::path!(String)
        .and(warp::get())
        .and(warp::query())
        .and_then(move |id, query| resolve(id, query, pool));

    // GET /{package}/{version}
    let download = warp::path!(String / Version)
        .and(warp::get())
        .and_then(|id, ver| download(id, ver, file_repo));
    // POST /{package}/version
    let upload = warp::path!(String / Version)
        .and(warp::post())
        .and(auth(pool))
        .and(warp::body::bytes())
        .and_then(move |id, ver, contents| upload(id, ver, contents, pool, file_repo));
    // DELETE /{package}/{version}
    let delete = warp::path!(String / Version)
        .and(warp::delete())
        .and(auth_admin(config))
        .and_then(move |id, ver| delete(id, ver, pool, config, file_repo));
    // POST /publish_key {key}
    let add_key = warp::path!("publish_key")
        .and(warp::post())
        .and(auth_admin(config))
        .and(warp::body::bytes())
        .and_then(move |contents| add_key(contents, pool));
    // POST /delete_key {key}
    let delete_key = warp::path!("delete_key")
        .and(warp::post())
        .and(auth_admin(config))
        .and(warp::body::bytes())
        .and_then(move |contents| delete_key(contents, pool));

    list.or(resolve)
        .or(download)
        .or(upload)
        .or(delete)
        .or(add_key)
        .or(delete_key)
        .recover(crate::errors::handle_rejection)
}

fn auth(
    pool: &'static SqlitePool,
) -> impl Filter<Extract = (), Error = Rejection> + Send + Sync + Clone + 'static {
    warp::header::optional("Authorization")
        .and_then(move |k: Option<HeaderValue>| async move {
            let k = match k {
                Some(k) => k,
                None => return Err(warp::reject::custom(crate::errors::Unauthorized)),
            };

            if PublishKey::resolve_one(k.to_str().or_ise()?, pool)
                .await
                .or_ise()?
                .is_some()
            {
                Ok(())
            } else {
                Err(warp::reject::custom(crate::errors::Unauthorized))
            }
        })
        .untuple_one()
}

fn auth_admin(
    config: &'static Config,
) -> impl Filter<Extract = (), Error = Rejection> + Send + Sync + Clone + 'static {
    warp::header::optional("Authorization")
        .and_then(move |k: Option<HeaderValue>| async move {
            let k = match k {
                Some(k) => k,
                None => return Err(warp::reject::custom(crate::errors::Unauthorized)),
            };

            if config.admin_keys.contains(k.to_str().or_ise()?) {
                Ok(())
            } else {
                Err(warp::reject::custom(crate::errors::Unauthorized))
            }
        })
        .untuple_one()
}

#[tracing::instrument(level = "debug", skip(pool))]
async fn list(pool: &SqlitePool) -> Result<impl Reply, Rejection> {
    Ok(warp::reply::json(&Mod::list(pool).await.or_ise()?))
}

#[tracing::instrument(level = "debug", skip(pool))]
async fn resolve(
    id: String,
    query: ResolveQuery,
    pool: &SqlitePool,
) -> Result<impl Reply, Rejection> {
    match query.limit {
        // 1 => last version, found or not found
        1 => Ok(warp::reply::json(
            &Mod::resolve_one(&id, &query.req, pool)
                .await
                .or_ise()?
                .or_nf()?,
        )),
        // 0 => all versions
        0 => Ok(warp::reply::json(
            &Mod::resolve_all(&id, &query.req, pool).await.or_ise()?,
        )),
        // n => n latest versions
        n => Ok(warp::reply::json(
            &Mod::resolve_n(&id, &query.req, pool, n).await.or_ise()?,
        )),
    }
}

#[tracing::instrument(level = "debug", skip(file_repo))]
async fn download(id: String, ver: Version, file_repo: &FileRepo) -> Result<impl Reply, Rejection> {
    let contents = file_repo.get_file(id, ver).await.or_nf()?;
    let reply = warp::reply::with_header(contents, CONTENT_TYPE, "application/json; charset=utf-8");
    Ok(reply)
}

#[tracing::instrument(level = "debug", skip(pool, file_repo))]
async fn upload(
    id: String,
    ver: Version,
    contents: Bytes,
    pool: &SqlitePool,
    file_repo: &FileRepo,
) -> Result<impl Reply, Rejection> {
    if !Mod::insert(&id, &ver, pool).await.or_ise()? {
        return Ok(warp::reply::with_status("", StatusCode::CONFLICT));
    }

    file_repo.write_file(id, ver, contents).await.or_ise()?;

    Ok(warp::reply::with_status("", StatusCode::CREATED))
}

#[tracing::instrument(level = "debug", skip(pool, config, file_repo))]
async fn delete(
    id: String,
    ver: Version,
    pool: &SqlitePool,
    config: &Config,
    file_repo: &FileRepo,
) -> Result<impl Reply, Rejection> {
    let mut dir = config
        .downloads_path
        .join(&id)
        .join(format!("{}/{}", ver.major, ver.minor));

    let file = dir.join(ver.patch.to_string());
    fs::remove_file(file).await.or_nf()?;
    // Then try to delete our directories, moving upwards
    for _ in 0..3 {
        if fs::remove_dir(&dir).await.is_err() {
            break;
        }
        dir = dir.parent().or_ise()?.to_path_buf();
    }
    Mod::delete(&id, &ver, pool).await.or_nf()?;

    file_repo.remove_file(id, ver).await.or_nf()?;

    Ok(warp::reply::with_status("", StatusCode::OK))
}

#[tracing::instrument(level = "debug", skip(pool))]
async fn add_key(contents: Bytes, pool: &SqlitePool) -> Result<impl Reply, Rejection> {
    let pub_key: PublishKey = serde_json::from_slice(&contents).or_ise()?;
    if !PublishKey::insert(&pub_key.user, &pub_key.pw, pool)
        .await
        .or_ise()?
    {
        return Ok(warp::reply::with_status("", StatusCode::CONFLICT));
    }

    Ok(warp::reply::with_status("", StatusCode::CREATED))
}

#[tracing::instrument(level = "debug", skip(pool))]
async fn delete_key(contents: Bytes, pool: &SqlitePool) -> Result<impl Reply, Rejection> {
    let pub_key: OptPublishKey = serde_json::from_slice(&contents).or_ise()?;

    if let Some(pw) = pub_key.pw {
        PublishKey::delete_pw(&pw, pool).await.or_nf()?;
        return Ok(warp::reply::with_status("", StatusCode::OK));
    } else if let Some(user) = pub_key.user {
        PublishKey::delete_user(&user, pool).await.or_nf()?;
        return Ok(warp::reply::with_status("", StatusCode::OK));
    }

    Ok(warp::reply::with_status("", StatusCode::BAD_REQUEST))
}
