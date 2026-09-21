package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoCanceladoPorConflitoEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompensacaoReacaoServiceTest {

    @Test
    void deveAplicarCompensacaoComIdempotenciaPorEventoId() {

        AgendamentoStatusService statusService = new AgendamentoStatusService();
        CompensacaoReacaoService service = new CompensacaoReacaoService(statusService);

        AgendamentoCanceladoPorConflitoEvent evento =
                new AgendamentoCanceladoPorConflitoEvent(
                        "COMP-EVT-1",
                        "AG-900",
                        "PROF-900",
                        "2026-09-21T10:00:00-03:00",
                        "Conflito de horario",
                        "2026-09-21T09:00:00-03:00"
                );

        statusService.marcarConfirmado("AG-900");

        service.processar(evento);
        service.processar(evento);

        assertEquals("CANCELADO_POR_CONFLITO", statusService.consultarStatus("AG-900"));
    }
}