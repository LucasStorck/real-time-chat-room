use std::{
    collections::{BTreeSet, VecDeque},
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{
        State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use shared::{ChatMessage, ClientMsg, ServerMsg};
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

const HISTORY_LEN: usize = 50;
const BROADCAST_CAPACITY: usize = 256;
const MAX_FRAME_BYTES: usize = 16 * 1024;

/// Rajada máxima de frames de texto por conexão.
const RATE_LIMIT_BURST: f64 = 5.0;
/// Ritmo sustentado de frames de texto por conexão, por segundo.
const RATE_LIMIT_PER_SEC: f64 = 5.0;
const RATE_LIMITED: &str = "Você está enviando mensagens rápido demais. Aguarde um instante.";

/// No shutdown, quanto tempo esperar o cliente responder o nosso `Close`.
const CLOSE_GRACE: Duration = Duration::from_secs(1);

/// Uma única sala em memória.
///
/// Todo envio no `broadcast` acontece com o lock de `state` adquirido; assim quem entra
/// recebe um `Welcome` consistente e não perde nem duplica mensagens.
pub struct ChatRoom {
    tx: broadcast::Sender<ServerMsg>,
    state: Mutex<RoomState>,
}

#[derive(Default)]
struct RoomState {
    users: BTreeSet<String>,
    history: VecDeque<ChatMessage>,
}

impl Default for ChatRoom {
    fn default() -> Self {
        Self {
            tx: broadcast::channel(BROADCAST_CAPACITY).0,
            state: Mutex::default(),
        }
    }
}

impl ChatRoom {
    fn lock(&self) -> MutexGuard<'_, RoomState> {
        self.state.lock().expect("mutex da sala envenenado")
    }

    /// Registra o usuário. Retorna `None` se o nome já estiver em uso.
    fn join(&self, name: &str) -> Option<(broadcast::Receiver<ServerMsg>, ServerMsg)> {
        let mut state = self.lock();
        if !state.users.insert(name.to_owned()) {
            return None;
        }
        // Anuncia antes de assinar, para o próprio usuário não receber o seu `UserJoined`.
        let _ = self.tx.send(ServerMsg::UserJoined {
            user: name.to_owned(),
        });
        let rx = self.tx.subscribe();
        let welcome = ServerMsg::Welcome {
            history: state.history.iter().cloned().collect(),
            users: state.users.iter().cloned().collect(),
        };
        Some((rx, welcome))
    }

    fn post(&self, user: &str, text: &str) {
        let msg = ChatMessage {
            user: user.to_owned(),
            text: text.to_owned(),
            ts: now_ms(),
        };
        let mut state = self.lock();
        if state.history.len() == HISTORY_LEN {
            state.history.pop_front();
        }
        state.history.push_back(msg.clone());
        let _ = self.tx.send(ServerMsg::Message(msg));
    }

    fn leave(&self, name: &str) {
        let mut state = self.lock();
        if state.users.remove(name) {
            let _ = self.tx.send(ServerMsg::UserLeft {
                user: name.to_owned(),
            });
        }
    }
}

/// Detecção de clientes mortos.
///
/// O server manda um ping a cada `ping_interval`. Se nenhum frame (pong, ping ou mensagem)
/// chegar do cliente em `idle_timeout`, a conexão é encerrada e o nome é liberado. Isso cobre
/// quedas abruptas de rede, em que o TCP não avisa que o outro lado sumiu.
/// `idle_timeout` precisa ser maior que `ping_interval`.
#[derive(Clone, Copy, Debug)]
pub struct Heartbeat {
    pub ping_interval: Duration,
    pub idle_timeout: Duration,
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

/// Encerramento gracioso.
///
/// No shutdown, o axum para de aceitar conexões, mas não acompanha os WebSockets: depois do
/// upgrade eles rodam em tasks próprias. Por isso cada socket fica registrado aqui. `trigger`
/// avisa todos para mandarem um `Close` (1001, "going away"), e `closed` espera que terminem.
#[derive(Clone, Default)]
pub struct Shutdown {
    token: CancellationToken,
    tracker: TaskTracker,
}

impl Shutdown {
    /// Pede que todas as conexões se encerrem. Pode ser chamado mais de uma vez.
    pub fn trigger(&self) {
        self.token.cancel();
        self.tracker.close();
    }

    pub fn is_triggered(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Completa assim que `trigger` for chamado.
    pub async fn triggered(&self) {
        self.token.cancelled().await;
    }

    /// Completa depois de `trigger`, quando todas as conexões tiverem terminado.
    pub async fn closed(&self) {
        self.tracker.wait().await;
    }
}

/// Token bucket: até `capacity` fichas, repostas a `refill_per_sec` por segundo.
struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_per_sec: f64, now: Instant) -> Self {
        Self {
            capacity,
            refill_per_sec,
            tokens: capacity,
            last: now,
        }
    }

    /// Consome uma ficha, se houver.
    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct AppState {
    room: Arc<ChatRoom>,
    heartbeat: Heartbeat,
    shutdown: Shutdown,
}

/// Monta o app: WebSocket em `/ws` e os arquivos do client (build do Trunk) no resto.
pub fn app(
    room: Arc<ChatRoom>,
    static_dir: &Path,
    heartbeat: Heartbeat,
    shutdown: Shutdown,
) -> Router {
    let spa = ServeDir::new(static_dir).fallback(ServeFile::new(static_dir.join("index.html")));
    Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(spa)
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            room,
            heartbeat,
            shutdown,
        })
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    if state.shutdown.is_triggered() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    // O registro acontece já no handler, e não dentro do `on_upgrade`, para que `closed`
    // também espere os upgrades que ainda estão em andamento.
    let tracked = state.shutdown.tracker.token();
    ws.max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            handle_socket(socket, state).await;
            drop(tracked);
        })
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let AppState {
        room,
        heartbeat,
        shutdown,
    } = state;
    let (mut sink, mut stream) = socket.split();
    let joined = await_join(
        &mut sink,
        &mut stream,
        &room,
        heartbeat.idle_timeout,
        &shutdown,
    )
    .await;
    let Some((name, rx)) = joined else {
        return;
    };
    tracing::info!(%name, "entrou na sala");

    // Mensagens só para este cliente (avisos), entregues pela mesma task que escreve no
    // socket. `read_client` usa `try_send`: se o canal encher, o aviso é descartado.
    let (direct_tx, direct_rx) = mpsc::channel(4);
    let mut send_task = tokio::spawn(forward_to_client(
        sink,
        rx,
        direct_rx,
        heartbeat.ping_interval,
        shutdown.clone(),
    ));
    let mut recv_task = tokio::spawn({
        let (room, name) = (room.clone(), name.clone());
        async move { read_client(stream, &room, &name, heartbeat.idle_timeout, direct_tx).await }
    });

    // Quando um lado termina (cliente fechou, erro de rede, shutdown), encerra o outro.
    tokio::select! {
        _ = &mut send_task => {
            if shutdown.is_triggered() {
                // Já mandamos o `Close`; dá ao cliente um instante para responder.
                let _ = tokio::time::timeout(CLOSE_GRACE, &mut recv_task).await;
            }
            recv_task.abort();
        }
        _ = &mut recv_task => send_task.abort(),
    }

    room.leave(&name);
    tracing::info!(%name, "saiu da sala");
}

/// Espera o `Join`, valida o nome e responde com `Welcome` ou `JoinRejected`.
async fn await_join(
    sink: &mut SplitSink<WebSocket, Message>,
    stream: &mut SplitStream<WebSocket>,
    room: &ChatRoom,
    idle_timeout: Duration,
    shutdown: &Shutdown,
) -> Option<(String, broadcast::Receiver<ServerMsg>)> {
    loop {
        // Só a espera pelo frame é interrompida pelo shutdown. Depois de `room.join`, o
        // `Welcome` é enviado até o fim, para o nome nunca ficar registrado numa conexão
        // abandonada no meio do caminho.
        let frame = tokio::select! {
            frame = next_frame(stream, idle_timeout) => frame,
            () = shutdown.triggered() => None,
        };
        let Some(frame) = frame else {
            if shutdown.is_triggered() {
                let _ = sink.send(going_away()).await;
            }
            return None;
        };
        let Message::Text(raw) = frame else { continue };
        let Ok(ClientMsg::Join { user }) = serde_json::from_str(raw.as_str()) else {
            continue;
        };

        let joined = shared::validate_username(&user).and_then(|name| {
            room.join(&name)
                .map(|(rx, welcome)| (name, rx, welcome))
                .ok_or_else(|| "Esse nome já está em uso.".to_owned())
        });

        return match joined {
            Ok((name, rx, welcome)) => {
                if send_json(sink, &welcome).await.is_err() {
                    room.leave(&name);
                    return None;
                }
                Some((name, rx))
            }
            Err(reason) => {
                let _ = send_json(sink, &ServerMsg::JoinRejected { reason }).await;
                let _ = sink.close().await;
                None
            }
        };
    }
}

/// Escreve no socket deste cliente: o broadcast da sala, os avisos diretos e os pings
/// periódicos. No shutdown, manda um `Close` e termina.
async fn forward_to_client(
    mut sink: SplitSink<WebSocket, Message>,
    mut rx: broadcast::Receiver<ServerMsg>,
    mut direct: mpsc::Receiver<ServerMsg>,
    ping_interval: Duration,
    shutdown: Shutdown,
) {
    let mut ping = tokio::time::interval(ping_interval);
    ping.tick().await; // o primeiro tick é imediato

    loop {
        let sent = tokio::select! {
            () = shutdown.triggered() => {
                let _ = sink.send(going_away()).await;
                break;
            }
            msg = rx.recv() => match msg {
                Ok(msg) => send_json(&mut sink, &msg).await,
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "cliente lento perdeu mensagens");
                    continue;
                }
                Err(RecvError::Closed) => break,
            },
            Some(msg) = direct.recv() => send_json(&mut sink, &msg).await,
            _ = ping.tick() => sink.send(Message::Ping(Default::default())).await,
        };
        if sent.is_err() {
            break;
        }
    }
}

/// Lê as mensagens do cliente e publica na sala.
///
/// Cada frame de texto consome uma ficha do `TokenBucket`. Sem ficha, o frame é descartado
/// e o cliente recebe um `ServerMsg::Error`, uma vez só por sequência de descartes, para
/// que um flood não vire um flood de avisos na volta.
async fn read_client(
    mut stream: SplitStream<WebSocket>,
    room: &ChatRoom,
    name: &str,
    idle_timeout: Duration,
    direct: mpsc::Sender<ServerMsg>,
) {
    let mut bucket = TokenBucket::new(RATE_LIMIT_BURST, RATE_LIMIT_PER_SEC, Instant::now());
    let mut warned = false;

    while let Some(frame) = next_frame(&mut stream, idle_timeout).await {
        match frame {
            Message::Text(raw) => {
                if !bucket.try_take(Instant::now()) {
                    if !warned {
                        tracing::debug!(%name, "rate limit atingido; descartando mensagens");
                        let _ = direct.try_send(ServerMsg::Error {
                            reason: RATE_LIMITED.to_owned(),
                        });
                        warned = true;
                    }
                    continue;
                }
                warned = false;

                match serde_json::from_str::<ClientMsg>(raw.as_str()) {
                    Ok(ClientMsg::Send { text }) => {
                        if let Some(text) = shared::validate_message(&text) {
                            room.post(name, text);
                        }
                    }
                    Ok(ClientMsg::Join { .. }) => {} // já entrou; ignora
                    Err(err) => tracing::debug!(%err, "mensagem inválida do cliente"),
                }
            }
            Message::Close(_) => break,
            _ => {} // ping/pong: o axum já responde; aqui só contam como sinal de vida
        }
    }
}

/// Próximo frame do cliente, ou `None` se a conexão fechou, deu erro ou ficou
/// `idle_timeout` sem receber nada.
async fn next_frame(
    stream: &mut SplitStream<WebSocket>,
    idle_timeout: Duration,
) -> Option<Message> {
    match tokio::time::timeout(idle_timeout, stream.next()).await {
        Ok(Some(Ok(frame))) => Some(frame),
        Ok(_) => None,
        Err(_) => {
            tracing::info!(?idle_timeout, "cliente sem resposta; encerrando a conexão");
            None
        }
    }
}

/// Frame `Close` enviado no shutdown. O client trata como queda e reconecta com backoff.
fn going_away() -> Message {
    Message::Close(Some(CloseFrame {
        code: close_code::AWAY,
        reason: "servidor encerrando".into(),
    }))
}

async fn send_json(
    sink: &mut SplitSink<WebSocket, Message>,
    msg: &ServerMsg,
) -> Result<(), axum::Error> {
    let json = serde_json::to_string(msg).expect("ServerMsg sempre serializa");
    sink.send(Message::Text(json.into())).await
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_allows_burst_then_refills() {
        let t0 = Instant::now();
        let ms = Duration::from_millis;
        let mut bucket = TokenBucket::new(5.0, 5.0, t0);

        for _ in 0..5 {
            assert!(bucket.try_take(t0));
        }
        assert!(!bucket.try_take(t0));

        // 5 fichas/s: em 100ms repõe meia ficha; em mais 150ms, passa de uma.
        assert!(!bucket.try_take(t0 + ms(100)));
        assert!(bucket.try_take(t0 + ms(250)));
        assert!(!bucket.try_take(t0 + ms(250)));

        // Muito tempo parado não acumula além da capacidade.
        let later = t0 + Duration::from_secs(60);
        for _ in 0..5 {
            assert!(bucket.try_take(later));
        }
        assert!(!bucket.try_take(later));
    }
}
