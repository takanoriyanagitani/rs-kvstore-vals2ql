use std::collections::HashMap;
use std::io;

use deadpool_redis::redis;

use redis::AsyncCommands;

use redis::FromRedisValue;
use redis::ToRedisArgs;

pub use deadpool_redis;

use deadpool_redis::Connection;

pub use async_graphql;

use async_graphql::Context;
use async_graphql::Object;

use async_graphql::dataloader::DataLoader;
use async_graphql::dataloader::Loader;
use async_graphql::dataloader::LruCache;

use async_graphql::EmptyMutation;
use async_graphql::EmptySubscription;
use async_graphql::Schema;

#[derive(Debug, Clone)]
pub enum KeyValLoadErr {
    UnableToGetKeys,
}

impl core::fmt::Display for KeyValLoadErr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            KeyValLoadErr::UnableToGetKeys => write!(f, "unable to get keys"),
        }
    }
}

impl std::error::Error for KeyValLoadErr {}

#[derive(Debug, Clone)]
pub struct RedisLike {
    pub pool: deadpool_redis::Pool,
}

impl RedisLike {
    pub async fn get_vals<K, V>(&self, keys: &[K]) -> Result<Vec<V>, io::Error>
    where
        K: ToRedisArgs + Sync,
        V: FromRedisValue,
    {
        let mut p: Connection = self.pool.get().await.map_err(io::Error::other)?;
        p.mget(keys).await.map_err(io::Error::other)
    }
}

#[async_trait::async_trait]
pub trait KeyValStore {
    async fn get_values<K, V>(&self, keys: &[K]) -> Result<Vec<V>, io::Error>
    where
        K: ToRedisArgs + Sync,
        V: FromRedisValue;
}

#[async_trait::async_trait]
impl KeyValStore for RedisLike {
    async fn get_values<K, V>(&self, keys: &[K]) -> Result<Vec<V>, io::Error>
    where
        K: ToRedisArgs + Sync,
        V: FromRedisValue,
    {
        self.get_vals(keys).await
    }
}

pub struct StringLoader {
    pub conn: RedisLike,
}

#[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
impl Loader<String> for StringLoader {
    type Value = String;
    type Error = KeyValLoadErr;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        println!("loading... key cnt: {}", keys.len());
        let vals: Vec<Option<String>> = self.conn.get_values(keys).await.map_err(|e| {
            eprintln!("{e}");
            KeyValLoadErr::UnableToGetKeys
        })?;
        // keys/values may not be "zip" compatible(wrong key/val pair possible)
        let i = keys.iter().zip(vals).map(|pair| {
            let (k, v) = pair;
            let ks: String = k.into();
            (ks, v.unwrap_or_default())
        });
        Ok(HashMap::from_iter(i))
    }
}

pub struct Query {
    pub conn: RedisLike,
}

#[Object]
impl Query {
    async fn values(&self, keys: Vec<String>) -> Result<Vec<String>, io::Error> {
        let c: RedisLike = self.conn.clone();
        let sl = StringLoader { conn: c };
        let dl = DataLoader::with_cache(sl, tokio::spawn, LruCache::new(100)).max_batch_size(2);
        let hm: HashMap<_, _> = dl.load_many(keys).await.map_err(io::Error::other)?;
        Ok(hm.into_values().collect())
    }

    async fn value(&self, ctx: &Context<'_>, key: String) -> Result<String, io::Error> {
        ctx.data_unchecked::<DataLoader<StringLoader>>()
            .load_one(key)
            .await
            .map(|o| o.unwrap_or_default())
            .map_err(io::Error::other)
    }
}

pub fn url2config(url: &str) -> deadpool_redis::Config {
    deadpool_redis::Config::from_url(url)
}

pub fn cfg2pool(cfg: &deadpool_redis::Config) -> Result<deadpool_redis::Pool, io::Error> {
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .map_err(io::Error::other)
}

pub fn url2pool(url: &str) -> Result<deadpool_redis::Pool, io::Error> {
    let cfg = url2config(url);
    cfg2pool(&cfg)
}

pub fn url2rlike(url: &str) -> Result<RedisLike, io::Error> {
    let cfg = url2config(url);
    let pool = cfg2pool(&cfg)?;
    Ok(RedisLike { pool })
}

pub fn url2query(url: &str) -> Result<Query, io::Error> {
    let rl = url2rlike(url)?;
    Ok(Query { conn: rl })
}

pub type KvsSchema = Schema<Query, EmptyMutation, EmptySubscription>;

pub fn query2schema(q: Query) -> KvsSchema {
    Schema::build(q, EmptyMutation, EmptySubscription).finish()
}

pub fn url2schema(u: &str) -> Result<KvsSchema, io::Error> {
    let q: Query = url2query(u)?;
    Ok(query2schema(q))
}
