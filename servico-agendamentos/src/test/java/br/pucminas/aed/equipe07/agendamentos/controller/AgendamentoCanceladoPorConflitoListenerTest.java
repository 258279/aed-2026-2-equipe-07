package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.service.CompensacaoReacaoService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AgendamentoCanceladoPorConflitoListenerTest {

    @Test
    void deveProcessarCompensacaoAntesDeConfirmarOffset() throws Exception {

        CompensacaoReacaoService service = mock(CompensacaoReacaoService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        AgendamentoCanceladoPorConflitoListener listener =
                new AgendamentoCanceladoPorConflitoListener(service, JsonMapper.builder().build());

        String json = """
                {
                    "eventoId": "COMP-001",
                    "agendamentoId": "AG-001",
                    "profissionalId": "PROF-001",
                    "inicioEm": "2026-09-21T10:00:00-03:00",
                    "motivo": "Conflito de horario",
                    "ocorridoEm": "2026-09-21T09:00:00-03:00"
                }
                """;

        ConsumerRecord<String, String> registro =
                new ConsumerRecord<>(
                        "salao.agendamento-cancelado-por-conflito",
                        0,
                        0L,
                        "AG-001",
                        json
                );

        listener.receber(registro, acknowledgment);

        InOrder ordem = inOrder(service, acknowledgment);
        ordem.verify(service).processar(any());
        ordem.verify(acknowledgment).acknowledge();
    }

    @Test
    void naoDeveConfirmarOffsetQuandoCompensacaoFalhar() throws Exception {

        CompensacaoReacaoService service = mock(CompensacaoReacaoService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        doThrow(new RuntimeException("falha transitoria"))
                .when(service)
                .processar(any());

        AgendamentoCanceladoPorConflitoListener listener =
                new AgendamentoCanceladoPorConflitoListener(service, JsonMapper.builder().build());

        String json = """
                {
                    "eventoId": "COMP-002",
                    "agendamentoId": "AG-002",
                    "profissionalId": "PROF-002",
                    "inicioEm": "2026-09-21T10:30:00-03:00",
                    "motivo": "Conflito de horario",
                    "ocorridoEm": "2026-09-21T09:10:00-03:00"
                }
                """;

        ConsumerRecord<String, String> registro =
                new ConsumerRecord<>(
                        "salao.agendamento-cancelado-por-conflito",
                        0,
                        0L,
                        "AG-002",
                        json
                );

        org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class,
                () -> listener.receber(registro, acknowledgment)
        );

        verify(acknowledgment, never()).acknowledge();
    }
}