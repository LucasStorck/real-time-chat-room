# Real Time Chat Room

Chat em tempo real em full Rust, organizado num Cargo workspace com três crates:

- `shared`: protocolo (`ClientMsg` / `ServerMsg`) e validações. Compila para nativo **e** para `wasm32`.
- `server`: Axum 0.8 + Tokio. WebSocket em `/ws`; serve `client/dist` em todas as outras rotas.
- `client`: Leptos 0.8 em CSR, compilado para WASM pelo Trunk. Usa gloo-net para o WebSocket.

A stack é um ponto de partida, não uma restrição: pode mudar conforme o projeto cresce. Qualquer troca precisa ser justificada pela necessidade, e os gatilhos estão em "Quando mudar a stack", no fim deste arquivo.

## Comandos

```bash
cargo test                                    # shared + testes e2e do server (WebSockets reais)
cargo clippy --all-targets                    # shared + server
cargo clippy -p client --target wasm32-unknown-unknown
cargo run -p server                           # :3000
cd client && trunk serve                      # :8080, faz proxy de /ws para :3000
cd client && trunk build --release            # gera client/dist, servido pelo server
```

`default-members` exclui o `client`: um `cargo build` ou `cargo test` na raiz **não** compila o client. Para checá-lo, use sempre `--target wasm32-unknown-unknown` ou o `trunk build`.

Variáveis do server: `ADDR` (padrão `127.0.0.1:3000`), `STATIC_DIR` (padrão `client/dist`), `RUST_LOG`.

## Regras do código

- **`shared` não pode depender de nada que não compile para WASM.** Nada de tokio, axum, sqlx ou `std::fs`. Só `serde` e lógica pura.
- **Toda mudança de protocolo começa em `shared`.** Cada nova variante de `ClientMsg` ou `ServerMsg` precisa de um teste de formato de fio em `shared/src/lib.rs` e de tratamento no server e no client.
- O JSON usa tag interna (`#[serde(tag = "type", rename_all = "snake_case")]`). Mantenha o formato estável, porque clientes antigos podem estar abertos durante um deploy.
- **Server: todo `tx.send` do broadcast acontece com o lock de `RoomState` adquirido.** É isso que garante que o `Welcome` (histórico + usuários) seja consistente com o que vem depois. O `join` envia `UserJoined` *antes* de `subscribe()`, para o próprio usuário não receber o próprio anúncio.
- Só `forward_to_client` escreve no socket depois do `Join`. Mensagens para um único cliente (como `ServerMsg::Error`) vão pelo canal `direct` da conexão, nunca pelo `broadcast`.
- Nunca segure `MutexGuard` (std) através de um `.await`. A lógica que precisa de lock fica em métodos síncronos de `ChatRoom`.
- O client valida com as mesmas funções do `shared` (`validate_username` e `validate_message`), mas o server **sempre** revalida.
- No client, o estado fica em `Chat`, uma struct `Copy` de signals. O loop de conexão é invalidado pelo contador `session`: nunca escreva em `outbox` a partir de uma sessão antiga.
- Mensagens para o usuário, logs e comentários ficam em português. Identificadores ficam em inglês.

## Como verificar mudanças

1. Rode `cargo test` e os dois `clippy` sem warnings.
2. Mudanças de comportamento no server ganham um teste em `server/tests/chat.rs`, que sobe o server numa porta livre e conecta com `tokio-tungstenite`.
3. Mudanças de UI precisam ser vistas no navegador: `trunk build`, `cargo run -p server` e dois usuários em abas separadas. Para automatizar, use Playwright **fora do repo**, num diretório temporário. Para derrubar o server num teste de reconexão, use o PID; nunca `pkill -f` com um padrão que apareça na própria linha de comando.

---

## Plano de implementação

Cada fase deve terminar com testes passando e o app funcionando ponta a ponta. Marque `[x]` ao concluir.

### Fase 0: MVP ✅

- [x] Workspace com `shared`, `server` e `client`
- [x] Uma sala (`# geral`) em memória, com broadcast e histórico das últimas 50 mensagens
- [x] Join com nome único, validação compartilhada e `JoinRejected`
- [x] Lista de usuários online, avisos de entrada e saída
- [x] Reconexão automática com backoff exponencial (0,5s → 10s) e indicador de status
- [x] Ping do server a cada 30s
- [x] Testes e2e do server e testes de formato de fio no `shared`

### Fase 1: Robustez e base do repositório

Objetivo: deixar o MVP confiável antes de crescer.

- [x] `git init`, primeiro commit e CI (GitHub Actions) com `cargo test`, os dois `clippy`, `cargo fmt --check` e `trunk build`
- [x] **Timeout de pong:** o server registra o último pong ou frame recebido de cada cliente e encerra a conexão após 60s sem resposta. Isso resolve o nome que fica "preso" quando a rede cai de forma abrupta. (Implementado como prazo de leitura em `next_frame`, configurável por `Heartbeat`; também vale para quem conecta e nunca envia `Join`.)
- [x] Manter o nome digitado no campo de login após um `JoinRejected` (hoje o `Login` remonta e limpa o campo)
- [x] Graceful shutdown do server (Ctrl+C / SIGTERM), fechando os sockets com um frame `Close`. (`Shutdown` combina um `CancellationToken` e um `TaskTracker`: cada socket manda `Close` 1001 e o `main` espera até 5s que todos terminem. Novos upgrades recebem 503.)
- [x] Rate limit simples por conexão (por exemplo, 5 mensagens/s, token bucket no `read_client`), respondendo com um novo `ServerMsg::Error { reason }`. (Rajada de 5 e 5/s sustentado. O excesso é descartado e o aviso vai uma vez por sequência de descartes.)

Critério de pronto: um teste e2e prova que um cliente que não responde é removido e o nome dele é liberado.

### Fase 2: Múltiplas salas

- [ ] `shared`: `ClientMsg::Join { user, room }` e, opcionalmente, `ClientMsg::ListRooms` / `ServerMsg::Rooms`
- [ ] `server`: um registro `Rooms` (`Mutex<HashMap<String, Arc<ChatRoom>>>`) que cria salas sob demanda e remove a sala vazia (sem usuários e sem histórico persistido)
- [ ] O nome é único **por sala**
- [ ] `client`: roteamento com `leptos_router` (`/r/:room`), lista de salas na barra lateral e troca de sala sem recarregar a página (uma nova sessão do loop de conexão)
- [ ] Validação do nome da sala no `shared` (slug: `[a-z0-9-]`, até 32 caracteres)

Decisão a tomar: uma conexão por sala (mais simples, é o modelo atual) ou uma conexão multiplexada com `room` em cada mensagem. Comece com uma conexão por sala.

### Fase 3: Persistência

- [ ] SQLx com **SQLite** primeiro, com migrations em `server/migrations/`
- [ ] Tabela `messages (id, room, user, text, ts)`. O `Welcome` carrega as últimas N mensagens do banco, e não mais da `VecDeque`.
- [ ] Gravação **fora do lock**: o `post` envia ao broadcast e uma task separada grava no banco (canal mpsc), para o banco não travar a sala
- [ ] `ChatMessage` ganha `id` (gerado pelo server). O client passa a usar esse `id` como chave do `<For>`.
- [ ] Paginação do histórico: `ClientMsg::LoadOlder { before_id }` → `ServerMsg::History { messages }`, com scroll infinito no client
- [ ] Testes com banco em memória (`sqlite::memory:`)

Migrar para **Postgres** apenas quando houver deploy com várias instâncias (fase 5). Com SQLx, a troca é basicamente a URL e o dialeto das migrations.

### Fase 4: Autenticação

- [ ] Rotas REST no Axum: `POST /api/register`, `POST /api/login` (hash com `argon2`) e `POST /api/logout`
- [ ] Sessão por cookie `HttpOnly` + `SameSite=Lax`. O handshake do `/ws` lê o cookie e rejeita com 401 antes do upgrade.
- [ ] O `Join` deixa de carregar `user`, porque o server usa a identidade da sessão
- [ ] `client`: telas de login e cadastro com `gloo-net::http`. Os tipos de request e response ficam no `shared`.
- [ ] Verificar `Origin` no upgrade do WebSocket (proteção contra CSWSH)

### Fase 5: Escala e deploy

- [ ] Dockerfile multi-stage: build do client com Trunk, build do server e imagem final enxuta com o binário + `dist`
- [ ] Postgres no lugar do SQLite
- [ ] **Várias instâncias do server:** Redis pub/sub como barramento entre instâncias (cada instância publica as mensagens da sala e repassa o que recebe ao seu `broadcast` local). A presença (quem está online) vai para o Redis, com TTL renovado pelo heartbeat.
- [ ] Métricas (`/metrics` com conexões ativas e mensagens por segundo) e logs estruturados em JSON
- [ ] Teste de carga simples (por exemplo, N clientes `tokio-tungstenite` num binário em `server/examples/`)

### Ideias (sem ordem)

Indicador de "digitando…", menções com `@nome`, mensagens não lidas por sala, edição e remoção de mensagens, upload de imagens, notificações do navegador, mensagens diretas.

---

## Quando mudar a stack

| Sinal | Mudança |
|---|---|
| Precisa de SEO, de primeiro carregamento rápido ou de páginas públicas | Trunk/CSR → **cargo-leptos** com SSR + hydration, server functions no Axum |
| Mensagens grandes ou volume alto, e JSON pesa no perfil | JSON → **postcard** ou **bincode** em frames binários (a mudança fica concentrada em `shared`) |
| Mais de uma instância do server | `broadcast` em memória → **Redis pub/sub**; SQLite → **Postgres** |
| A UI cresce e a equipe prefere outro modelo | Avaliar Dioxus (`dx`). O `shared` e o server continuam iguais. |
