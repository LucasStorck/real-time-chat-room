//! Protocolo compartilhado entre `server` e `client`.
//!
//! Tudo aqui precisa compilar tanto para nativo quanto para `wasm32-unknown-unknown`,
//! então nada de tokio, axum ou acesso a sistema de arquivos.

use serde::{Deserialize, Serialize};

pub const MAX_USERNAME_LEN: usize = 24;
pub const MAX_MESSAGE_LEN: usize = 1000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub user: String,
    pub text: String,
    /// Unix epoch em milissegundos, definido pelo servidor.
    pub ts: i64,
}

/// Mensagens enviadas pelo navegador ao servidor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Primeira mensagem obrigatória após abrir o WebSocket.
    Join {
        user: String,
    },
    Send {
        text: String,
    },
}

/// Mensagens enviadas pelo servidor ao navegador.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Resposta a um `Join` aceito: histórico recente e quem está online.
    Welcome {
        history: Vec<ChatMessage>,
        users: Vec<String>,
    },
    /// Resposta a um `Join` recusado; o servidor fecha a conexão em seguida.
    JoinRejected {
        reason: String,
    },
    Message(ChatMessage),
    UserJoined {
        user: String,
    },
    UserLeft {
        user: String,
    },
    /// Aviso para este cliente apenas (por exemplo, mensagens rápidas demais).
    /// A conexão continua aberta.
    Error {
        reason: String,
    },
}

/// Normaliza e valida um nome de usuário. Usada pelos dois lados.
pub fn validate_username(raw: &str) -> Result<String, String> {
    let name = raw.trim();
    if name.is_empty() {
        return Err("O nome não pode ficar vazio.".into());
    }
    if name.chars().count() > MAX_USERNAME_LEN {
        return Err(format!(
            "O nome pode ter no máximo {MAX_USERNAME_LEN} caracteres."
        ));
    }
    if name.chars().any(char::is_control) {
        return Err("O nome contém caracteres inválidos.".into());
    }
    Ok(name.to_owned())
}

/// Retorna o texto normalizado se a mensagem for válida para envio.
pub fn validate_message(raw: &str) -> Option<&str> {
    let text = raw.trim();
    (!text.is_empty() && text.chars().count() <= MAX_MESSAGE_LEN).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format_is_tagged_json() {
        let msg = ClientMsg::Join { user: "ana".into() };
        assert_eq!(
            serde_json::to_string(&msg).unwrap(),
            r#"{"type":"join","user":"ana"}"#
        );

        let msg = ServerMsg::Message(ChatMessage {
            user: "ana".into(),
            text: "oi".into(),
            ts: 1,
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"type":"message","user":"ana","text":"oi","ts":1}"#
        );
        assert_eq!(serde_json::from_str::<ServerMsg>(&json).unwrap(), msg);

        let msg = ServerMsg::Error {
            reason: "devagar".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"error","reason":"devagar"}"#);
        assert_eq!(serde_json::from_str::<ServerMsg>(&json).unwrap(), msg);
    }

    #[test]
    fn username_validation() {
        assert_eq!(validate_username("  ana  "), Ok("ana".into()));
        assert!(validate_username("   ").is_err());
        assert!(validate_username(&"a".repeat(MAX_USERNAME_LEN + 1)).is_err());
        assert!(validate_username("a\nb").is_err());
    }

    #[test]
    fn message_validation() {
        assert_eq!(validate_message("  oi \n"), Some("oi"));
        assert_eq!(validate_message("   "), None);
        assert_eq!(validate_message(&"x".repeat(MAX_MESSAGE_LEN + 1)), None);
    }
}
