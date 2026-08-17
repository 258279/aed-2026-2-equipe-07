-- Init DB for servico-ocupacao
CREATE TABLE IF NOT EXISTS eventos_processados (
    evento_id VARCHAR(100) PRIMARY KEY,
    processado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS projecao_ocupacao (
    agendamento_id VARCHAR(100) PRIMARY KEY,
    profissional_id VARCHAR(100) NOT NULL,
    inicio_em VARCHAR(40) NOT NULL
);
