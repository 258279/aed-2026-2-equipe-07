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
class AgendamentoEventStoreTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoEventStore eventStore;

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

        banco.update("DELETE FROM agendamento_eventos");
    }

    @Test
    void deveGravarECarregarStreamNaOrdemDeVersao() {

        eventStore.gravar(List.of(
                new EventoAgendamento(
                        "AG-600",
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        "{}",
                        "2026-09-01T10:00:00-03:00")
        ));

        eventStore.gravar(List.of(
                new EventoAgendamento(
                        "AG-600",
                        2,
                        Agendamento.TIPO_CANCELADO,
                        "{}",
                        "2026-09-02T10:00:00-03:00"),
                new EventoAgendamento(
                        "AG-600",
                        3,
                        Agendamento.TIPO_SINAL_ESTORNADO,
                        "{}",
                        "2026-09-02T10:00:00-03:00")
        ));

        List<EventoAgendamento> stream =
                eventStore.carregarStream("AG-600");

        assertEquals(3, stream.size());
        assertEquals(1, stream.get(0).getVersao());
        assertEquals(2, stream.get(1).getVersao());
        assertEquals(3, stream.get(2).getVersao());
        assertEquals(
                Agendamento.TIPO_CONFIRMADO,
                stream.get(0).getTipoEvento()
        );
    }

    @Test
    void deveLancarConflitoAoGravarVersaoJaExistente() {

        eventStore.gravar(List.of(
                new EventoAgendamento(
                        "AG-601",
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        "{}",
                        "2026-09-01T10:00:00-03:00")
        ));

        assertThrows(
                ConflitoDeVersaoException.class,
                () -> eventStore.gravar(List.of(
                        new EventoAgendamento(
                                "AG-601",
                                1,
                                Agendamento.TIPO_CANCELADO,
                                "{}",
                                "2026-09-01T11:00:00-03:00")
                ))
        );
    }
}
