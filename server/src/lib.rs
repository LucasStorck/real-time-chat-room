use std::{
    collections::{BTreeSet, VecDeque},
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use shared::{ChatMessage, ClientMsg, ServerMsg};
use tokio::sync::broadcast::{self, error::RecvError};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

const HISTORY_LEN: usize = 50;
const BROADCAST_CAPACITY: usize = 256;
const MAX_FRAME_BYTES: usize = 16 * 1024;

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

#[derive(Clone)]
struct AppState {
    room: Arc<ChatRoom>,
    heartbeat: Heartbeat,
}

/// Monta o app: WebSocket em `/ws` e os arquivos do client (build do Trunk) no resto.
pub fn app(room: Arc<ChatRoom>, static_dir: &Path, heartbeat: Heartbeat) -> Router {
    let spa = ServeDir::new(static_dir).fallback(ServeFile::new(static_dir.join("index.html")));
    Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(spa)
        .layer(TraceLayer::new_for_http())
        .with_state(AppState { room, heartbeat })
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, AppState { room, heartbeat }: AppState) {
    let (mut sink, mut stream) = socket.split();
    let Some((name, rx)) = await_join(&mut sink, &mut stream, &room, heartbeat.idle_timeout).await
    else {
        return;
    };
    tracing::info!(%name, "entrou na sala");

    let mut send_task = tokio::spawn(forward_broadcast(sink, rx, heartbeat.ping_interval));
    let mut recv_task = tokio::spawn({
        let (room, name) = (room.clone(), name.clone());
        async move { read_client(stream, &room, &name, heartbeat.idle_timeout).await }
    });

    // Quando um lado termina (cliente fechou, erro de rede), encerra o outro.
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
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
) -> Option<(String, broadcast::Receiver<ServerMsg>)> {
    while let Some(frame) = next_frame(stream, idle_timeout).await {
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
    None
}

/// Repassa o broadcast da sala para este cliente e manda pings periódicos.
async fn forward_broadcast(
    mut sink: SplitSink<WebSocket, Message>,
    mut rx: broadcast::Receiver<ServerMsg>,
    ping_interval: Duration,
) {
    let mut ping = tokio::time::interval(ping_interval);
    ping.tick().await; // o primeiro tick é imediato

    loop {
        let sent = tokio::select! {
            msg = rx.recv() => match msg {
                Ok(msg) => send_json(&mut sink, &msg).await,
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "cliente lento perdeu mensagens");
                    continue;
                }
                Err(RecvError::Closed) => break,
            },
            _ = ping.tick() => sink.send(Message::Ping(Default::default())).await,
        };
        if sent.is_err() {
            break;
        }
    }
}

/// Lê as mensagens do cliente e publica na sala.
async fn read_client(
    mut stream: SplitStream<WebSocket>,
    room: &ChatRoom,
    name: &str,
    idle_timeout: Duration,
) {
    while let Some(frame) = next_frame(&mut stream, idle_timeout).await {
        match frame {
            Message::Text(raw) => match serde_json::from_str::<ClientMsg>(raw.as_str()) {
                Ok(ClientMsg::Send { text }) => {
                    if let Some(text) = shared::validate_message(&text) {
                        room.post(name, text);
                    }
                }
                Ok(ClientMsg::Join { .. }) => {} // já entrou; ignora
                Err(err) => tracing::debug!(%err, "mensagem inválida do cliente"),
            },
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
