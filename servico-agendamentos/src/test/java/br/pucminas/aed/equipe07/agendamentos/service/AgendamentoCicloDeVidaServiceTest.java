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
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
class AgendamentoCicloDeVidaServiceTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoEventStore eventStore;

    @Autowired
    private ProjetorFaltasEstornos projetor;

    @Autowired
    private AgendamentoCicloDeVidaService cicloDeVidaService;

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
    void deveCancelarUmAgendamentoConfirmadoEGravarOsDoisEventos() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-800", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        cicloDeVidaService.cancelar("AG-800");

        List<EventoAgendamento> stream = eventStore.carregarStream("AG-800");

        assertEquals(3, stream.size());
        assertEquals(Agendamento.TIPO_CANCELADO, stream.get(1).getTipoEvento());
        assertEquals(Agendamento.TIPO_SINAL_ESTORNADO, stream.get(2).getTipoEvento());
    }

    @Test
    void deveMarcarNaoComparecimentoEGravarOsDoisEventos() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-801", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        cicloDeVidaService.marcarNaoComparecimento("AG-801");

        List<EventoAgendamento> stream = eventStore.carregarStream("AG-801");

        assertEquals(3, stream.size());
        assertEquals(Agendamento.TIPO_NAO_COMPARECEU, stream.get(1).getTipoEvento());
        assertEquals(Agendamento.TIPO_MULTA_COBRADA, stream.get(2).getTipoEvento());
    }

    @Test
    void naoDeveCancelarUmAgendamentoQueJaFoiCancelado() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-802", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        cicloDeVidaService.cancelar("AG-802");

        assertThrows(IllegalStateException.class, () -> cicloDeVidaService.cancelar("AG-802"));
    }
}
