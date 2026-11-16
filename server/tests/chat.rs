//! Testes de ponta a ponta: sobem o servidor numa porta livre e conectam clientes WebSocket reais.

use std::{path::Path, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use server::{ChatRoom, Heartbeat, Shutdown};
use shared::{ClientMsg, ServerMsg};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn spawn_server() -> String {
    spawn_server_with(Heartbeat::default()).await
}

async fn spawn_server_with(heartbeat: Heartbeat) -> String {
    spawn_server_full(heartbeat).await.0
}

/// Sobe o server e devolve também o `Shutdown`, para os testes de encerramento.
async fn spawn_server_full(heartbeat: Heartbeat) -> (String, Shutdown) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Shutdown::default();
    let app = server::app(
        Arc::new(ChatRoom::default()),
        Path::new("/nonexistent"),
        heartbeat,
        shutdown.clone(),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("ws://{addr}/ws"), shutdown)
}

async fn send(ws: &mut Ws, msg: ClientMsg) {
    let json = serde_json::to_string(&msg).unwrap();
    ws.send(Message::Text(json.into())).await.unwrap();
}

/// Próxima mensagem de texto, pulando pings. O prazo vale para a espera toda, e não
/// por frame: com heartbeat curto chegam pings o tempo todo e um prazo por frame nunca venceria.
async fn recv(ws: &mut Ws) -> ServerMsg {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = ws.next().await.expect("conexão encerrada").unwrap();
            if let Message::Text(text) = frame {
                return serde_json::from_str(&text).unwrap();
            }
        }
    })
    .await
    .expect("timeout esperando mensagem")
}

/// Todas as mensagens de texto que chegarem até o socket ficar `quiet` sem receber nada.
async fn drain(ws: &mut Ws, quiet: Duration) -> Vec<ServerMsg> {
    let mut msgs = Vec::new();
    while let Ok(Some(frame)) = timeout(quiet, ws.next()).await {
        if let Message::Text(text) = frame.unwrap() {
            msgs.push(serde_json::from_str(&text).unwrap());
        }
    }
    msgs
}

fn count_messages(msgs: &[ServerMsg]) -> usize {
    let is_message = |m: &&ServerMsg| matches!(m, ServerMsg::Message(_));
    msgs.iter().filter(is_message).count()
}

/// Espera o frame `Close` do server e devolve o código dele.
async fn close_code(ws: &mut Ws) -> Option<u16> {
    timeout(Duration::from_secs(2), async {
        while let Some(frame) = ws.next().await {
            match frame {
                Ok(Message::Close(frame)) => return frame.map(|f| u16::from(f.code)),
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
        None
    })
    .await
    .expect("timeout esperando o Close")
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

#[tokio::test]
async fn unresponsive_client_is_dropped_and_name_is_freed() {
    let url = spawn_server_with(Heartbeat {
        ping_interval: Duration::from_millis(100),
        idle_timeout: Duration::from_millis(300),
    })
    .await;

    let (mut ana, _) = join(&url, "ana").await;
    // `bia` continua com o TCP aberto, mas nunca mais lê o socket: o tungstenite só
    // responde pings quando o stream é lido, então ela se comporta como uma rede que caiu.
    let (_bia, _) = join(&url, "bia").await;
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserJoined { user: "bia".into() }
    );

    // `ana` fica ociosa mas segue lendo (e respondendo pings), então não deve cair.
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserLeft { user: "bia".into() }
    );

    // O nome foi liberado e `ana` continua conectada para ver a nova entrada.
    let (_, welcome) = join(&url, "bia").await;
    assert_eq!(users(&welcome), ["ana", "bia"]);
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserJoined { user: "bia".into() }
    );
}

#[tokio::test]
async fn connection_that_never_joins_is_closed() {
    let url = spawn_server_with(Heartbeat {
        ping_interval: Duration::from_millis(100),
        idle_timeout: Duration::from_millis(300),
    })
    .await;

    let (mut ws, _) = connect_async(&url).await.unwrap();
    let end = timeout(Duration::from_secs(2), async {
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
    assert!(end.is_ok(), "o server deveria ter fechado a conexão");
}

#[tokio::test]
async fn shutdown_sends_close_frame_and_waits_for_connections() {
    let (url, shutdown) = spawn_server_full(Heartbeat::default()).await;
    let (mut ana, _) = join(&url, "ana").await;
    // Conexão que ainda não mandou `Join` também precisa ser encerrada.
    let (mut pending, _) = connect_async(&url).await.unwrap();

    shutdown.trigger();

    // 1001 = "going away": o client entende como queda e reconecta com backoff.
    assert_eq!(close_code(&mut ana).await, Some(1001));
    assert_eq!(close_code(&mut pending).await, Some(1001));
    timeout(Duration::from_secs(2), shutdown.closed())
        .await
        .expect("as conexões deveriam ter terminado");

    // Depois do shutdown, novos upgrades são recusados.
    assert!(connect_async(&url).await.is_err());
}

#[tokio::test]
async fn flooding_client_is_rate_limited() {
    const FLOOD: usize = 20;
    let quiet = Duration::from_millis(300);
    let url = spawn_server().await;
    let (mut ana, _) = join(&url, "ana").await;
    let (mut bia, _) = join(&url, "bia").await;
    assert_eq!(
        recv(&mut ana).await,
        ServerMsg::UserJoined { user: "bia".into() }
    );

    for i in 0..FLOOD {
        send(
            &mut ana,
            ClientMsg::Send {
                text: format!("m{i}"),
            },
        )
        .await;
    }

    let to_ana = drain(&mut ana, quiet).await;
    let accepted = count_messages(&to_ana);
    // A rajada é de 5; o refil durante o envio pode liberar mais uma ou outra.
    assert!((5..FLOOD).contains(&accepted), "aceitas: {accepted}");
    let warned = to_ana.iter().any(|m| matches!(m, ServerMsg::Error { .. }));
    assert!(warned, "esperava um aviso de rate limit: {to_ana:?}");

    // O aviso é só para quem enviou; os outros veem apenas as mensagens aceitas.
    let to_bia = drain(&mut bia, quiet).await;
    assert_eq!(count_messages(&to_bia), accepted);
    assert_eq!(to_bia.len(), accepted, "{to_bia:?}");

    // A conexão continua aberta e o limite se recupera com o tempo.
    tokio::time::sleep(Duration::from_millis(500)).await;
    send(
        &mut ana,
        ClientMsg::Send {
            text: "depois".into(),
        },
    )
    .await;
    match recv(&mut ana).await {
        ServerMsg::Message(m) => assert_eq!(m.text, "depois"),
        other => panic!("esperava Message, veio {other:?}"),
    }
}
