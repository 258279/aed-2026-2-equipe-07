package br.pucminas.aed.equipe07.agendamentos.domain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AgendamentoConfirmadoEventTest {

    @Test
    void deveManterOsDadosDoEvento() {

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "SERV-004",
                        "2026-08-20T14:00:00-03:00",
                        "PADRAO",
                        "2026-08-16T02:55:00-03:00"
                );

        assertEquals("EVT-001", evento.getEventoId());
        assertEquals("AG-001", evento.getAgendamentoId());
        assertEquals("PROF-018", evento.getProfissionalId());
        assertEquals("SERV-004", evento.getServicoId());
        assertEquals(
                "2026-08-20T14:00:00-03:00",
                evento.getInicioEm()
        );
        assertEquals("PADRAO", evento.getPrioridade());
        assertEquals(
                "2026-08-16T02:55:00-03:00",
                evento.getOcorridoEm()
        );
    }
}