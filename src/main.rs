use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::HeaderMap;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use reqwest::Client;
use serde::Serialize;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

const ALLOWED_ORIGINS: &[&str] = &["monochrome.tf", "samidy.com"];

fn is_origin_allowed(origin: &str) -> bool {
    if let Ok(url) = origin.parse::<hyper::Uri>() {
        if let Some(host) = url.host() {
            return ALLOWED_ORIGINS.iter().any(|allowed| {
                host == *allowed || host.ends_with(&format!(".{})", allowed))
            });
        }
    }
    false
}

struct Backend {
    url: String,
    cooldown_until: RwLock<Option<Instant>>,
}

#[derive(Clone, Serialize)]
struct BackendHealth {
    url: String,
    status_code: Option<u16>,
    healthy: bool,
    last_check: String,
    error: Option<String>,
}

#[derive(Clone)]
struct CachedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    expires_at: Instant,
}

struct LoadBalancer {
    backends: Vec<Arc<Backend>>,
    counter: AtomicUsize,
    client: Client,
    cooldown_duration: Duration,
    cache: RwLock<HashMap<String, CachedResponse>>,
    cache_duration: Duration,
    health_status: RwLock<Vec<BackendHealth>>,
}

impl LoadBalancer {
    fn new(urls: Vec<String>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(100)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build client");

        let health_status: Vec<BackendHealth> = urls
            .iter()
            .map(|url| BackendHealth {
                url: url.clone(),
                status_code: None,
                healthy: false,
                last_check: "never".to_string(),
                error: None,
            })
            .collect();

        let backends = urls
            .into_iter()
            .map(|url| {
                Arc::new(Backend {
                    url,
                    cooldown_until: RwLock::new(None),
                })
            })
            .collect();

        Self {
            backends,
            counter: AtomicUsize::new(0),
            client,
            cooldown_duration: Duration::from_secs(600),
            cache: RwLock::new(HashMap::new()),
            cache_duration: Duration::from_secs(600),
            health_status: RwLock::new(health_status),
        }
    }

    fn cache_key(method: &Method, path: &str) -> String {
        format!("{}:{}", method, path)
    }

    fn get_cached(&self, method: &Method, path: &str) -> Option<CachedResponse> {
        let key = Self::cache_key(method, path);
        let cache = self.cache.read();
        if let Some(cached) = cache.get(&key) {
            if Instant::now() < cached.expires_at {
                return Some(cached.clone());
            }
        }
        None
    }

    fn store_cached(&self, method: &Method, path: &str, response: CachedResponse) {
        let key = Self::cache_key(method, path);
        let mut cache = self.cache.write();
        cache.insert(key, response);
    }

    fn cleanup_expired_cache(&self) {
        let now = Instant::now();
        let mut cache = self.cache.write();
        cache.retain(|_, v| v.expires_at > now);
    }

    fn get_next_backend(&self) -> Option<Arc<Backend>> {
        let len = self.backends.len();
        let now = Instant::now();

        for _ in 0..len {
            let idx = self.counter.fetch_add(1, Ordering::Relaxed) % len;
            let backend = &self.backends[idx];

            let cooldown = backend.cooldown_until.read();
            if let Some(until) = *cooldown {
                if now < until {
                    continue;
                }
            }
            drop(cooldown);

            return Some(Arc::clone(backend));
        }
        None
    }

    fn mark_failed(&self, backend: &Backend) {
        let mut cooldown = backend.cooldown_until.write();
        *cooldown = Some(Instant::now() + self.cooldown_duration);
    }

    async fn check_all_backends(&self) {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
        let mut new_health: Vec<BackendHealth> = Vec::new();

        for backend in &self.backends {
            let health = match self.client.get(&backend.url).send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let healthy = resp.status().is_success();
                    
                    if !healthy {
                        self.mark_failed(backend);
                    } else {
                        let mut cooldown = backend.cooldown_until.write();
                        *cooldown = None;
                    }

                    BackendHealth {
                        url: backend.url.clone(),
                        status_code: Some(status),
                        healthy,
                        last_check: now.clone(),
                        error: None,
                    }
                }
                Err(e) => {
                    self.mark_failed(backend);
                    BackendHealth {
                        url: backend.url.clone(),
                        status_code: None,
                        healthy: false,
                        last_check: now.clone(),
                        error: Some(e.to_string()),
                    }
                }
            };
            new_health.push(health);
        }

        let mut health_status = self.health_status.write();
        *health_status = new_health;
    }

    fn get_health_response(&self, origin: Option<&str>) -> Response<Full<Bytes>> {
        let health = self.health_status.read().clone();
        let all_healthy = health.iter().all(|h| h.healthy);

        let json = serde_json::json!({
            "status": if all_healthy { "healthy" } else { "degraded" },
            "backends": health
        });

        let body = serde_json::to_string_pretty(&json).unwrap();
        let status = if all_healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        let mut response = Response::builder()
            .status(status)
            .header("Content-Type", "application/json");
        
        if let Some(origin_str) = origin {
            response = response.header("Access-Control-Allow-Origin", origin_str);
            response = response.header("Access-Control-Allow-Credentials", "true");
        }
        
        response.body(Full::new(Bytes::from(body))).unwrap()
    }

    async fn forward_request(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let method = req.method().clone();
        let path_and_query = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str().to_owned())
            .unwrap_or_else(|| "/".to_owned());

        let origin = req.headers().get(hyper::header::ORIGIN)
            .and_then(|v| v.to_str().ok());

        if let Some(origin_str) = origin {
            if !is_origin_allowed(origin_str) {
                return Ok(Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Full::new(Bytes::from("CORS: Origin not allowed")))
                    .unwrap());
            }
        }

        if method == Method::OPTIONS {
            if let Some(origin_str) = origin {
                return Ok(Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header("Access-Control-Allow-Origin", origin_str)
                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS")
                    .header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
                    .header("Access-Control-Max-Age", "86400")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            }
        }

    if path_and_query == "/health" && (method == Method::GET || method == Method::HEAD) {
        let response = self.get_health_response(origin);
        if method == Method::HEAD {
            // Return headers and status but with empty body
            let (parts, _) = response.into_parts();
            return Ok(Response::from_parts(parts, Full::new(Bytes::new())));
        }
        return Ok(response);
}
        let origin_value = origin.unwrap_or("*").to_string();

        if let Some(cached) = self.get_cached(&method, &path_and_query) {
            let mut response = Response::builder().status(cached.status);
            for (name, value) in cached.headers.iter() {
                response = response.header(name.clone(), value.clone());
            }
            response = response.header("X-Cache", "HIT");
            if origin.is_some() {
                response = response.header("Access-Control-Allow-Origin", &origin_value);
                response = response.header("Access-Control-Allow-Credentials", "true");
            }
            return Ok(response.body(Full::new(cached.body)).unwrap());
        }

        let headers = req.headers().clone();
        let body_bytes = match req.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Failed to read request body")))
                    .unwrap());
            }
        };

        let mut tried: HashSet<String> = HashSet::new();

        loop {
            let backend = match self.get_next_backend() {
                Some(b) => b,
                None => {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Full::new(Bytes::from("No available backends")))
                        .unwrap());
                }
            };

            if tried.contains(&backend.url) {
                if tried.len() >= self.backends.len() {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Full::new(Bytes::from("All backends exhausted")))
                        .unwrap());
                }
                continue;
            }
            tried.insert(backend.url.clone());

            let target_url = format!("{}{}", backend.url, path_and_query);

            let mut request_builder = match method {
                Method::GET => self.client.get(&target_url),
                Method::POST => self.client.post(&target_url),
                Method::PUT => self.client.put(&target_url),
                Method::DELETE => self.client.delete(&target_url),
                Method::PATCH => self.client.patch(&target_url),
                Method::HEAD => self.client.head(&target_url),
                _ => self.client.request(method.clone(), &target_url),
            };

            for (name, value) in headers.iter() {
                if name == hyper::header::HOST || name == hyper::header::CONTENT_LENGTH {
                    continue;
                }
                request_builder = request_builder.header(name.clone(), value.clone());
            }

            if !body_bytes.is_empty() {
                request_builder = request_builder.body(body_bytes.clone());
            }

            match request_builder.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let resp_headers = resp.headers().clone();

                    match resp.bytes().await {
                        Ok(resp_body) => {
                            let mut filtered_headers = HeaderMap::new();
                            for (name, value) in resp_headers.iter() {
                                if name != hyper::header::TRANSFER_ENCODING {
                                    filtered_headers.insert(name.clone(), value.clone());
                                }
                            }

                            if status.is_success() {
                                let cached = CachedResponse {
                                    status,
                                    headers: filtered_headers.clone(),
                                    body: resp_body.clone(),
                                    expires_at: Instant::now() + self.cache_duration,
                                };
                                self.store_cached(&method, &path_and_query, cached);
                            }

                            let mut response = Response::builder().status(status);
                            for (name, value) in filtered_headers.iter() {
                                response = response.header(name.clone(), value.clone());
                            }
                            response = response.header("X-Cache", "MISS");
                            if origin.is_some() {
                                response = response.header("Access-Control-Allow-Origin", &origin_value);
                                response = response.header("Access-Control-Allow-Credentials", "true");
                            }

                            return Ok(response.body(Full::new(resp_body)).unwrap());
                        }
                        Err(_) => {
                            self.mark_failed(&backend);
                            continue;
                        }
                    }
                }
                Err(_) => {
                    self.mark_failed(&backend);
                    continue;
                }
            }
        }
    }
}

fn create_tcp_listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nodelay(true)?;
    socket.set_nonblocking(true)?;

    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(20));
    socket.set_tcp_keepalive(&keepalive)?;

    socket.bind(&addr.into())?;
    socket.listen(8192)?;

    Ok(socket.into())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let region = std::env::var("REGION")
        .expect("REGION environment variable not set")
        .to_lowercase();

    let config_path = format!("./regions/{}.json", region);
    let config_content =
        std::fs::read_to_string(&config_path).expect("Failed to read config file");
    let urls: Vec<String> =
        serde_json::from_str(&config_content).expect("Failed to parse config file");

    let lb = Arc::new(LoadBalancer::new(urls));
    let addr: SocketAddr = "0.0.0.0:3500".parse().unwrap();

    println!("Performing initial health check on all backends...");
    lb.check_all_backends().await;
    println!("Initial health check complete");

    let lb_health = Arc::clone(&lb);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(600));
        interval.tick().await;
        loop {
            interval.tick().await;
            println!("Running scheduled health check...");
            lb_health.check_all_backends().await;
            lb_health.cleanup_expired_cache();
            println!("Health check complete");
        }
    });

    let std_listener = create_tcp_listener(addr).expect("Failed to create listener");
    let listener = TcpListener::from_std(std_listener).expect("Failed to convert listener");

    println!("Load balancer listening on {}", addr);

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => continue,
        };

        let _ = stream.set_nodelay(true);
        let lb = Arc::clone(&lb);

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(|req| {
                let lb = Arc::clone(&lb);
                async move { lb.forward_request(req).await }
            });

            let _ = http1::Builder::new()
                .keep_alive(true)
                .pipeline_flush(true)
                .serve_connection(io, service)
                .await;
        });
    }
}