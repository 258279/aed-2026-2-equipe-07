package br.pucminas.aed.equipe07.ocupacao.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AgendamentoConfirmadoEventTest {

    @Test
    void deveManterSomenteOsDadosNecessariosParaOcupacao() {

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "2026-08-20T14:00:00-03:00"
                );

        assertEquals("EVT-001", evento.getEventoId());
        assertEquals("AG-001", evento.getAgendamentoId());
        assertEquals("PROF-018", evento.getProfissionalId());
        assertEquals(
                "2026-08-20T14:00:00-03:00",
                evento.getInicioEm()
        );
    }

    @Test
    void deveIgnorarCamposDesconhecidosDoProdutor() throws Exception {

        String json = """
            {
                "eventoId": "EVT-001",
                "agendamentoId": "AG-001",
                "profissionalId": "PROF-018",
                "servicoId": "SERV-004",
                "inicioEm": "2026-08-20T14:00:00-03:00",
                "prioridade": "PADRAO",
                "ocorridoEm": "2026-08-16T12:30:00-03:00"
            }
            """;

        tools.jackson.databind.ObjectMapper mapper =
                new tools.jackson.databind.ObjectMapper();

        AgendamentoConfirmadoEvent evento =
                mapper.readValue(
                        json,
                        AgendamentoConfirmadoEvent.class
                );

        assertEquals("EVT-001", evento.getEventoId());
        assertEquals("AG-001", evento.getAgendamentoId());
        assertEquals("PROF-018", evento.getProfissionalId());
        assertEquals(
                "2026-08-20T14:00:00-03:00",
                evento.getInicioEm()
        );
    }
}