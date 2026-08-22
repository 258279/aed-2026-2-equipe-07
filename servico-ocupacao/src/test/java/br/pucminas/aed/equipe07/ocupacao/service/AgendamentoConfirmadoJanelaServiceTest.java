package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEventDetalhado;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class AgendamentoConfirmadoJanelaServiceTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoConfirmadoJanelaService service;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS agregacao_confirmacoes_por_janela (
                    janela_inicio VARCHAR(40) NOT NULL,
                    janela_fim VARCHAR(40) NOT NULL,
                    prioridade VARCHAR(20) NOT NULL,
                    quantidade_confirmacoes INTEGER NOT NULL,
                    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (janela_inicio, prioridade)
                )
                """);

        banco.update("DELETE FROM agregacao_confirmacoes_por_janela");
    }

    @Test
    void deveAgruparPelaJanelaDeQuinzeMinutosBaseadaNoTempoDeOcorrencia() {

        AgendamentoConfirmadoEventDetalhado primeiroEvento =
                new AgendamentoConfirmadoEventDetalhado(
                        "EVT-100",
                        "AG-100",
                        "PROF-100",
                        "SERV-100",
                        "2026-08-22T14:00:00-03:00",
                        "URGENTE",
                        "2026-08-16T12:07:00-03:00"
                );

        AgendamentoConfirmadoEventDetalhado segundoEvento =
                new AgendamentoConfirmadoEventDetalhado(
                        "EVT-101",
                        "AG-101",
                        "PROF-101",
                        "SERV-101",
                        "2026-08-22T14:15:00-03:00",
                        "URGENTE",
                        "2026-08-16T12:14:59-03:00"
                );

        service.processar(primeiroEvento);
        service.processar(segundoEvento);

        Integer quantidadeConfirmacoes = banco.queryForObject(
                """
                SELECT quantidade_confirmacoes
                FROM agregacao_confirmacoes_por_janela
                WHERE janela_inicio = ? AND prioridade = ?
                """,
                Integer.class,
                "2026-08-16T12:00:00-03:00",
                "URGENTE"
        );

        String janelaFim = banco.queryForObject(
                """
                SELECT janela_fim
                FROM agregacao_confirmacoes_por_janela
                WHERE janela_inicio = ? AND prioridade = ?
                """,
                String.class,
                "2026-08-16T12:00:00-03:00",
                "URGENTE"
        );

        assertEquals(2, quantidadeConfirmacoes);
        assertEquals("2026-08-16T12:15:00-03:00", janelaFim);
    }

    @Test
    void deveClassificarEventoTardioPelaHoraDeOcorrencia() {

        AgendamentoConfirmadoEventDetalhado eventoTardio =
                new AgendamentoConfirmadoEventDetalhado(
                        "EVT-102",
                        "AG-102",
                        "PROF-102",
                        "SERV-102",
                        "2026-08-22T14:00:00-03:00",
                        "PADRAO",
                        "2026-08-16T11:53:00-03:00"
                );

        service.processar(eventoTardio);

        Integer quantidadeConfirmacoes = banco.queryForObject(
                """
                SELECT quantidade_confirmacoes
                FROM agregacao_confirmacoes_por_janela
                WHERE janela_inicio = ? AND prioridade = ?
                """,
                Integer.class,
                "2026-08-16T11:45:00-03:00",
                "PADRAO"
        );

        assertEquals(1, quantidadeConfirmacoes);
    }
}