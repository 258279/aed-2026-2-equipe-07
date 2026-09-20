CREATE DATABASE agendamentos;

\c agendamentos

CREATE TABLE IF NOT EXISTS agendamento_eventos (
    agendamento_id VARCHAR(100) NOT NULL,
    versao INTEGER NOT NULL,
    tipo_evento VARCHAR(60) NOT NULL,
    payload TEXT NOT NULL,
    ocorrido_em VARCHAR(40) NOT NULL,
    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agendamento_id, versao)
);
