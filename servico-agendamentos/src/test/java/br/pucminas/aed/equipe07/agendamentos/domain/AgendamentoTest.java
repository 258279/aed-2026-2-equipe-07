package br.pucminas.aed.equipe07.agendamentos.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AgendamentoTest {

    @Test
    void deveReconstruirComoConfirmadoAPartirDoEventoDeConfirmacao() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento(
                        "AG-700",
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        "{}",
                        "2026-09-01T10:00:00-03:00")
        ));

        assertEquals(Agendamento.Status.CONFIRMADO, agendamento.getStatus());
        assertEquals(1, agendamento.getVersaoAtual());
    }

    @Test
    void deveGerarCanceladoESinalEstornadoAoCancelarUmConfirmado() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento(
                        "AG-701",
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        "{}",
                        "2026-09-01T10:00:00-03:00")
        ));

        List<EventoAgendamento> novosEventos =
                agendamento.cancelar("2026-09-02T09:00:00-03:00");

        assertEquals(2, novosEventos.size());
        assertEquals(2, novosEventos.get(0).getVersao());
        assertEquals(
                Agendamento.TIPO_CANCELADO,
                novosEventos.get(0).getTipoEvento()
        );
        assertEquals(3, novosEventos.get(1).getVersao());
        assertEquals(
                Agendamento.TIPO_SINAL_ESTORNADO,
                novosEventos.get(1).getTipoEvento()
        );
    }

    @Test
    void deveGerarNaoCompareceuEMultaCobradaAoMarcarNaoComparecimento() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento(
                        "AG-703",
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        "{}",
                        "2026-09-01T10:00:00-03:00")
        ));

        List<EventoAgendamento> novosEventos =
                agendamento.marcarNaoComparecimento(
                        "2026-09-01T18:00:00-03:00"
                );

        assertEquals(2, novosEventos.size());
        assertEquals(
                Agendamento.TIPO_NAO_COMPARECEU,
                novosEventos.get(0).getTipoEvento()
        );
        assertEquals(
                Agendamento.TIPO_MULTA_COBRADA,
                novosEventos.get(1).getTipoEvento()
        );
    }

    @Test
    void naoDevePermitirCancelarUmAgendamentoJaCancelado() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento(
                        "AG-702",
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        "{}",
                        "2026-09-01T10:00:00-03:00"),
                new EventoAgendamento(
                        "AG-702",
                        2,
                        Agendamento.TIPO_CANCELADO,
                        "{}",
                        "2026-09-02T09:00:00-03:00"),
                new EventoAgendamento(
                        "AG-702",
                        3,
                        Agendamento.TIPO_SINAL_ESTORNADO,
                        "{}",
                        "2026-09-02T09:00:00-03:00")
        ));

        assertThrows(
                IllegalStateException.class,
                () -> agendamento.cancelar("2026-09-03T09:00:00-03:00")
        );
    }
}
