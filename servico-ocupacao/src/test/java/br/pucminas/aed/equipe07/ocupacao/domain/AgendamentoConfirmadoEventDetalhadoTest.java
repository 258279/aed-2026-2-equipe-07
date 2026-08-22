package br.pucminas.aed.equipe07.ocupacao.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AgendamentoConfirmadoEventDetalhadoTest {

    @Test
    void deveManterTodosOsDadosDoEvento() {

        AgendamentoConfirmadoEventDetalhado evento =
                new AgendamentoConfirmadoEventDetalhado(
                        "EVT-002",
                        "AG-002",
                        "PROF-019",
                        "SERV-010",
                        "2026-08-22T14:00:00-03:00",
                        "URGENTE",
                        "2026-08-16T12:37:00-03:00"
                );

        assertEquals("EVT-002", evento.getEventoId());
        assertEquals("AG-002", evento.getAgendamentoId());
        assertEquals("PROF-019", evento.getProfissionalId());
        assertEquals("SERV-010", evento.getServicoId());
        assertEquals("2026-08-22T14:00:00-03:00", evento.getInicioEm());
        assertEquals("URGENTE", evento.getPrioridade());
        assertEquals("2026-08-16T12:37:00-03:00", evento.getOcorridoEm());
    }
}