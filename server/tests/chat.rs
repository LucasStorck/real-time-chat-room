//! Testes de ponta a ponta: sobem o servidor numa porta livre e conectam clientes WebSocket reais.

use std::{path::Path, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use server::ChatRoom;
use shared::{ClientMsg, ServerMsg};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn spawn_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::app(Arc::new(ChatRoom::default()), Path::new("/nonexistent"));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{addr}/ws")
}

async fn send(ws: &mut Ws, msg: ClientMsg) {
    let json = serde_json::to_string(&msg).unwrap();
    ws.send(Message::Text(json.into())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> ServerMsg {
    loop {
        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout esperando mensagem")
            .expect("conexão encerrada")
            .unwrap();
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

async fn join(url: &str, name: &str) -> (Ws, ServerMsg) {
    let (mut ws, _) = connect_async(url).await.unwrap();
    send(&mut ws, ClientMsg::Join { user: name.into() }).await;
    let reply = recv(&mut ws).await;
    (ws, reply)
}

fn users(welcome: &ServerMsg) -> Vec<String> {
    match welcome {
        ServerMsg::Welcome { users, .. } => users.clone(),
        other => panic!("esperava Welcome, veio {other:?}"),
    }
}

#[tokio::test]
async fn messages_are_broadcast_and_kept_in_history() {
    let url = spawn_server().await;

    let (mut ana, welcome) = join(&url, "ana").await;
    assert_eq!(users(&welcome), ["ana"]);

    let (mut bia, welcome) = join(&url, "bia").await;
    assert_eq!(users(&welcome), ["ana", "bia"]);
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserJoined { user: "bia".into() }
    );

    send(
        &mut ana,
        ClientMsg::Send {
            text: "  oi!  ".into(),
        },
    )
    .await;
    for ws in [&mut ana, &mut bia] {
        match recv(ws).await {
            ServerMsg::Message(m) => assert_eq!((m.user.as_str(), m.text.as_str()), ("ana", "oi!")),
            other => panic!("esperava Message, veio {other:?}"),
        }
    }

    // Quem entra depois recebe o histórico.
    let (_caio, welcome) = join(&url, "caio").await;
    let ServerMsg::Welcome { history, .. } = welcome else {
        unreachable!()
    };
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].text, "oi!");
}

#[tokio::test]
async fn duplicate_and_invalid_names_are_rejected() {
    let url = spawn_server().await;
    let (_ana, _) = join(&url, "ana").await;

    let (_, reply) = join(&url, "ana").await;
    assert!(matches!(reply, ServerMsg::JoinRejected { .. }), "{reply:?}");

    let (_, reply) = join(&url, "   ").await;
    assert!(matches!(reply, ServerMsg::JoinRejected { .. }), "{reply:?}");
}

#[tokio::test]
async fn disconnect_announces_user_left_and_frees_name() {
    let url = spawn_server().await;
    let (mut ana, _) = join(&url, "ana").await;
    let (bia, _) = join(&url, "bia").await;
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserJoined { user: "bia".into() }
    );

    drop(bia);
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserLeft { user: "bia".into() }
    );

    let (_, welcome) = join(&url, "bia").await;
    assert_eq!(users(&welcome), ["ana", "bia"]);
}
