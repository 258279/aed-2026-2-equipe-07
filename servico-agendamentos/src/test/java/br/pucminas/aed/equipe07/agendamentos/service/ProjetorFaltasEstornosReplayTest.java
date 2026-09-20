package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class ProjetorFaltasEstornosReplayTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoEventStore eventStore;

    @Autowired
    private ProjetorFaltasEstornos projetor;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_eventos (
                    agendamento_id VARCHAR(100) NOT NULL,
                    versao INTEGER NOT NULL,
                    tipo_evento VARCHAR(60) NOT NULL,
                    payload TEXT NOT NULL,
                    ocorrido_em VARCHAR(40) NOT NULL,
                    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (agendamento_id, versao)
                )
                """);

        banco.execute("""
                CREATE TABLE IF NOT EXISTS relatorio_faltas_estornos (
                    data VARCHAR(10) NOT NULL,
                    tipo VARCHAR(20) NOT NULL,
                    quantidade INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (data, tipo)
                )
                """);

        banco.update("DELETE FROM agendamento_eventos");
        banco.update("DELETE FROM relatorio_faltas_estornos");
    }

    @Test
    void deveReconstruirAProjecaoIdenticaAposApagarTudoEReprocessarOLog() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-501", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        Agendamento agendamento1 = Agendamento.reconstruir(eventStore.carregarStream("AG-501"));
        List<EventoAgendamento> cancelamento = agendamento1.cancelar("2026-09-02T09:00:00-03:00");
        eventStore.gravar(cancelamento);
        projetor.processar(cancelamento);

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-502", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T11:00:00-03:00")
        ));

        Agendamento agendamento2 = Agendamento.reconstruir(eventStore.carregarStream("AG-502"));
        List<EventoAgendamento> naoComparecimento = agendamento2.marcarNaoComparecimento("2026-09-01T18:00:00-03:00");
        eventStore.gravar(naoComparecimento);
        projetor.processar(naoComparecimento);

        Integer estornosAntes = contar("2026-09-02", "ESTORNO");
        Integer multasAntes = contar("2026-09-01", "MULTA");

        assertEquals(1, estornosAntes);
        assertEquals(1, multasAntes);

        banco.update("DELETE FROM relatorio_faltas_estornos");

        projetor.reconstruirDoZero();

        Integer estornosDepois = contar("2026-09-02", "ESTORNO");
        Integer multasDepois = contar("2026-09-01", "MULTA");

        assertEquals(estornosAntes, estornosDepois);
        assertEquals(multasAntes, multasDepois);
    }

    private Integer contar(String data, String tipo) {
        return banco.queryForObject(
                "SELECT quantidade FROM relatorio_faltas_estornos WHERE data = ? AND tipo = ?",
                Integer.class, data, tipo
        );
    }
}
