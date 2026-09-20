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

CREATE TABLE IF NOT EXISTS relatorio_faltas_estornos (
    data VARCHAR(10) NOT NULL,
    tipo VARCHAR(20) NOT NULL,
    quantidade INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (data, tipo)
);
