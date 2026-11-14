use std::{
    cell::Cell,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::{
    SinkExt, StreamExt,
    channel::mpsc::{self, UnboundedSender},
    future,
};
use gloo_net::websocket::{Message, futures::WebSocket};
use gloo_timers::future::TimeoutFuture;
use leptos::{ev::SubmitEvent, html, prelude::*, task::spawn_local};
use shared::{ChatMessage, ClientMsg, MAX_MESSAGE_LEN, MAX_USERNAME_LEN, ServerMsg};

const MAX_ENTRIES: usize = 500;
const BACKOFF_START_MS: u32 = 500;
const BACKOFF_MAX_MS: u32 = 10_000;

fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(App);
}

// ---------------------------------------------------------------------------
// Estado
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Connecting,
    Online,
    Offline,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Status::Connecting => "conectando…",
            Status::Online => "online",
            Status::Offline => "reconectando…",
        }
    }

    fn class(self) -> &'static str {
        match self {
            Status::Connecting => "badge connecting",
            Status::Online => "badge online",
            Status::Offline => "badge offline",
        }
    }
}

#[derive(Clone)]
struct Entry {
    id: u64,
    kind: EntryKind,
}

#[derive(Clone)]
enum EntryKind {
    Chat(ChatMessage),
    System(String),
}

impl Entry {
    fn new(kind: EntryKind) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            kind,
        }
    }
}

/// Estado reativo do chat. Todos os campos são handles `Copy` do Leptos.
#[derive(Clone, Copy)]
struct Chat {
    user: RwSignal<Option<String>>,
    status: RwSignal<Status>,
    entries: RwSignal<Vec<Entry>>,
    online: RwSignal<Vec<String>>,
    login_error: RwSignal<Option<String>>,
    login_name: RwSignal<String>,
    /// Canal para o socket atual (`None` enquanto desconectado).
    outbox: StoredValue<Option<UnboundedSender<ClientMsg>>>,
    /// Incrementa a cada entrada/saída; loops de conexão antigos se encerram ao notar a mudança.
    session: StoredValue<u64>,
}

impl Chat {
    fn new() -> Self {
        Self {
            user: RwSignal::new(None),
            status: RwSignal::new(Status::Connecting),
            entries: RwSignal::new(Vec::new()),
            online: RwSignal::new(Vec::new()),
            login_error: RwSignal::new(None),
            login_name: RwSignal::new(String::new()),
            outbox: StoredValue::new(None),
            session: StoredValue::new(0),
        }
    }

    fn join(self, name: String) {
        let session = self.bump_session();
        self.login_error.set(None);
        self.entries.set(Vec::new());
        self.online.set(Vec::new());
        self.user.set(Some(name.clone()));
        spawn_local(self.connection_loop(session, name));
    }

    fn leave(self) {
        self.bump_session();
        self.outbox.set_value(None); // fecha o writer e, com ele, o socket
        self.user.set(None);
    }

    /// Envia uma mensagem; retorna `true` se ela foi aceita para envio.
    fn send(self, raw: &str) -> bool {
        let Some(text) = shared::validate_message(raw) else {
            return false;
        };
        let msg = ClientMsg::Send {
            text: text.to_owned(),
        };
        self.outbox
            .with_value(|tx| tx.as_ref().is_some_and(|tx| tx.unbounded_send(msg).is_ok()))
    }

    fn bump_session(self) -> u64 {
        self.session.update_value(|s| *s += 1);
        self.session.get_value()
    }

    fn is_session(self, session: u64) -> bool {
        self.session.get_value() == session
    }

    fn push(self, entry: EntryKind) {
        self.entries.update(|entries| {
            entries.push(Entry::new(entry));
            if entries.len() > MAX_ENTRIES {
                entries.drain(..entries.len() - MAX_ENTRIES);
            }
        });
    }

    /// Mantém a conexão viva, reconectando com backoff exponencial.
    async fn connection_loop(self, session: u64, name: String) {
        let mut backoff_ms = BACKOFF_START_MS;

        while self.is_session(session) {
            self.status.set(Status::Connecting);
            let welcomed = Cell::new(false);

            match WebSocket::open(&ws_url()) {
                Ok(ws) => {
                    let (mut sink, mut stream) = ws.split();
                    let (tx, mut rx) = mpsc::unbounded();
                    let _ = tx.unbounded_send(ClientMsg::Join { user: name.clone() });
                    self.outbox.set_value(Some(tx));

                    let writer = async move {
                        while let Some(msg) = rx.next().await {
                            let json =
                                serde_json::to_string(&msg).expect("ClientMsg sempre serializa");
                            if sink.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    };
                    let reader = async {
                        while let Some(Ok(frame)) = stream.next().await {
                            let Message::Text(json) = frame else { continue };
                            match serde_json::from_str::<ServerMsg>(&json) {
                                Ok(msg) => self.handle(msg, &welcomed),
                                Err(err) => leptos::logging::warn!("mensagem inválida: {err}"),
                            }
                        }
                    };
                    // Termina quando o servidor fecha (reader) ou quando saímos (writer).
                    let _ = future::select(Box::pin(writer), Box::pin(reader)).await;

                    if self.is_session(session) {
                        self.outbox.set_value(None);
                    }
                }
                Err(err) => leptos::logging::warn!("falha ao abrir WebSocket: {err}"),
            }

            if !self.is_session(session) {
                break;
            }
            if welcomed.get() {
                backoff_ms = BACKOFF_START_MS;
            }
            self.status.set(Status::Offline);
            TimeoutFuture::new(backoff_ms).await;
            backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
        }
    }

    fn handle(self, msg: ServerMsg, welcomed: &Cell<bool>) {
        match msg {
            ServerMsg::Welcome { history, users } => {
                welcomed.set(true);
                let history = history.into_iter().map(|m| Entry::new(EntryKind::Chat(m)));
                self.entries.set(history.collect());
                self.online.set(users);
                self.status.set(Status::Online);
            }
            ServerMsg::JoinRejected { reason } => {
                self.login_error.set(Some(reason));
                self.leave();
            }
            ServerMsg::Message(m) => self.push(EntryKind::Chat(m)),
            ServerMsg::UserJoined { user } => {
                self.push(EntryKind::System(format!("{user} entrou na sala")));
                self.online.update(|users| {
                    users.push(user);
                    users.sort();
                });
            }
            ServerMsg::UserLeft { user } => {
                self.push(EntryKind::System(format!("{user} saiu da sala")));
                self.online.update(|users| users.retain(|u| *u != user));
            }
        }
    }
}

/// `ws://` ou `wss://` no mesmo host da página: funciona tanto via proxy do Trunk quanto servido pelo Axum.
fn ws_url() -> String {
    let location = web_sys::window().expect("sem window").location();
    let scheme = match location.protocol().as_deref() {
        Ok("https:") => "wss",
        _ => "ws",
    };
    format!("{scheme}://{}/ws", location.host().unwrap_or_default())
}

fn format_time(ts_ms: i64) -> String {
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(ts_ms as f64));
    format!("{:02}:{:02}", date.get_hours(), date.get_minutes())
}

// ---------------------------------------------------------------------------
// Componentes
// ---------------------------------------------------------------------------

#[component]
fn App() -> impl IntoView {
    let chat = Chat::new();
    view! {
        <Show
            when=move || chat.user.with(Option::is_some)
            fallback=move || view! { <Login chat=chat /> }
        >
            <Room chat=chat />
        </Show>
    }
}

#[component]
fn Login(chat: Chat) -> impl IntoView {
    let name = chat.login_name;

    let on_submit = move |ev: SubmitEvent| {
        ev.prevent_default();
        match shared::validate_username(&name.get_untracked()) {
            Ok(name) => chat.join(name),
            Err(err) => chat.login_error.set(Some(err)),
        }
    };

    view! {
        <main class="login">
            <form class="card" on:submit=on_submit>
                <h1>"Chat Room"</h1>
                <label for="username">"Como você quer ser chamado?"</label>
                <input
                    id="username"
                    type="text"
                    autocomplete="off"
                    autofocus
                    placeholder="seu nome"
                    maxlength=MAX_USERNAME_LEN.to_string()
                    prop:value=move || name.get()
                    on:input=move |ev| name.set(event_target_value(&ev))
                />
                {move || chat.login_error.get().map(|err| view! { <p class="error">{err}</p> })}
                <button type="submit">"Entrar"</button>
            </form>
        </main>
    }
}

#[component]
fn Room(chat: Chat) -> impl IntoView {
    let draft = RwSignal::new(String::new());
    let list_ref = NodeRef::<html::Div>::new();
    let me = chat.user.get_untracked().unwrap_or_default();
    let offline = move || chat.status.get() != Status::Online;

    // Rola para a última mensagem sempre que a lista muda.
    Effect::new(move |_| {
        chat.entries.track();
        if let Some(el) = list_ref.get() {
            el.set_scroll_top(el.scroll_height());
        }
    });

    let on_submit = move |ev: SubmitEvent| {
        ev.prevent_default();
        if chat.send(&draft.get_untracked()) {
            draft.set(String::new());
        }
    };

    view! {
        <div class="room">
            <header class="topbar">
                <h1>"# geral"</h1>
                <span class=move || chat.status.get().class()>{move || chat.status.get().label()}</span>
                <span class="spacer"></span>
                <span class="me">{me.clone()}</span>
                <button class="ghost" on:click=move |_| chat.leave()>"Sair"</button>
            </header>

            <aside class="users">
                <h2>"Online · " {move || chat.online.with(Vec::len)}</h2>
                <ul>
                    <For
                        each=move || chat.online.get()
                        key=|user| user.clone()
                        children=move |user| view! { <li>{user}</li> }
                    />
                </ul>
            </aside>

            <div class="messages" node_ref=list_ref>
                <For
                    each=move || chat.entries.get()
                    key=|entry| entry.id
                    children=move |entry| entry_view(entry, &me)
                />
            </div>

            <form class="composer" on:submit=on_submit>
                <input
                    type="text"
                    autocomplete="off"
                    placeholder="Escreva uma mensagem…"
                    maxlength=MAX_MESSAGE_LEN.to_string()
                    prop:value=move || draft.get()
                    on:input=move |ev| draft.set(event_target_value(&ev))
                    disabled=offline
                />
                <button type="submit" disabled=offline>"Enviar"</button>
            </form>
        </div>
    }
}

fn entry_view(entry: Entry, me: &str) -> AnyView {
    match entry.kind {
        EntryKind::System(text) => view! { <p class="system">{text}</p> }.into_any(),
        EntryKind::Chat(msg) => {
            let mine = msg.user == me;
            view! {
                <div class="msg" class:mine=mine>
                    <div class="meta">
                        <span class="author">{msg.user}</span>
                        <time>{format_time(msg.ts)}</time>
                    </div>
                    <p class="text">{msg.text}</p>
                </div>
            }
            .into_any()
        }
    }
}
