package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class OcupacaoServiceTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private OcupacaoService service;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS eventos_processados (
                    evento_id VARCHAR(100) PRIMARY KEY,
                    processado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """);

        banco.execute("""
                CREATE TABLE IF NOT EXISTS projecao_ocupacao (
                    agendamento_id VARCHAR(100) PRIMARY KEY,
                    profissional_id VARCHAR(100) NOT NULL,
                    inicio_em VARCHAR(40) NOT NULL
                )
                """);

        banco.update("DELETE FROM eventos_processados");
        banco.update("DELETE FROM projecao_ocupacao");
    }

    @Test
    void deveProcessarMesmoEventoTresVezesComEfeitoUnico() {

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "2026-08-20T14:00:00-03:00"
                );

        service.processar(evento);
        service.processar(evento);
        service.processar(evento);

        Integer quantidadeEventos =
                banco.queryForObject(
                        """
                        SELECT COUNT(*)
                        FROM eventos_processados
                        WHERE evento_id = ?
                        """,
                        Integer.class,
                        "EVT-001"
                );

        Integer quantidadeOcupacoes =
                banco.queryForObject(
                        """
                        SELECT COUNT(*)
                        FROM projecao_ocupacao
                        WHERE agendamento_id = ?
                        """,
                        Integer.class,
                        "AG-001"
                );

        assertEquals(1, quantidadeEventos);
        assertEquals(1, quantidadeOcupacoes);
    }

    @Test
    void deveDesfazerDeduplicacaoQuandoEfeitoFalhar() {

        AgendamentoConfirmadoEvent primeiroEvento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "2026-08-20T14:00:00-03:00"
                );

        service.processar(primeiroEvento);

        AgendamentoConfirmadoEvent segundoEvento =
                new AgendamentoConfirmadoEvent(
                        "EVT-002",
                        "AG-001",
                        "PROF-018",
                        "2026-08-20T14:00:00-03:00"
                );

        org.junit.jupiter.api.Assertions.assertThrows(
                Exception.class,
                () -> service.processar(segundoEvento)
        );

        Integer segundoEventoRegistrado =
                banco.queryForObject(
                        """
                        SELECT COUNT(*)
                        FROM eventos_processados
                        WHERE evento_id = ?
                        """,
                        Integer.class,
                        "EVT-002"
                );

        Integer quantidadeOcupacoes =
                banco.queryForObject(
                        """
                        SELECT COUNT(*)
                        FROM projecao_ocupacao
                        WHERE agendamento_id = ?
                        """,
                        Integer.class,
                        "AG-001"
                );

        assertEquals(0, segundoEventoRegistrado);
        assertEquals(1, quantidadeOcupacoes);
    }
}