# Real Time Chat Room

Chat em tempo real escrito todo em Rust.

| Crate | O que é |
|---|---|
| [`shared`](shared/src/lib.rs) | Protocolo (`ClientMsg` / `ServerMsg`) e validações usadas pelos dois lados |
| [`server`](server/src/lib.rs) | Axum + Tokio: WebSocket em `/ws`, sala em memória com `broadcast`, serve o build do client |
| [`client`](client/src/main.rs) | Leptos (CSR/WASM) + gloo-net, com reconexão automática |

## Pré-requisitos

```bash
rustup target add wasm32-unknown-unknown
cargo install trunk --locked
```

## Desenvolvimento

Dois terminais:

```bash
cargo run -p server            # http://127.0.0.1:3000 (WebSocket em /ws)
```

```bash
cd client && trunk serve       # http://127.0.0.1:8080, com hot reload; /ws é encaminhado para o server
```

Abra http://127.0.0.1:8080 em duas abas e entre com nomes diferentes.

## Produção (um binário só)

```bash
cd client && trunk build --release && cd ..
cargo run -p server --release  # serve client/dist e o WebSocket na porta 3000
```

Variáveis: `ADDR` (padrão `127.0.0.1:3000`), `STATIC_DIR` (padrão `client/dist`), `RUST_LOG`.

## Testes

```bash
cargo test                     # shared + testes de ponta a ponta do server com WebSockets reais
```

## Protocolo

JSON com tag `type`, por exemplo:

```json
→ {"type":"join","user":"ana"}
← {"type":"welcome","history":[...],"users":["ana"]}
→ {"type":"send","text":"oi"}
← {"type":"message","user":"ana","text":"oi","ts":1763071234567}
```

O primeiro frame do cliente precisa ser `join`. Se o nome for inválido ou já estiver em uso, o servidor responde `join_rejected` e fecha a conexão.

O servidor manda um ping a cada 30s e encerra a conexão de quem fica 60s sem enviar nenhum frame (nem pong), liberando o nome.

Cada conexão pode mandar uma rajada de 5 mensagens e depois 5 por segundo. O excesso é descartado e quem enviou recebe um aviso (`{"type":"error","reason":"..."}`); a conexão continua aberta.

No Ctrl+C ou SIGTERM, o servidor fecha cada WebSocket com um frame `Close` (código 1001) e espera até 5s antes de sair. O client trata isso como queda e reconecta sozinho quando o servidor volta.

## Próximos passos

- Várias salas (`HashMap<String, ChatRoom>`) e `room` no `Join`
- Persistência do histórico (SQLx + SQLite/Postgres)
- Autenticação no handshake do WebSocket
- Redis pub/sub para rodar várias instâncias do server
