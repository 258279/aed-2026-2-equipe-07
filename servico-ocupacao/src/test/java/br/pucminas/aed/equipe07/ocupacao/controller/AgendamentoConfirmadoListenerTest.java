package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.ocupacao.service.OcupacaoService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AgendamentoConfirmadoListenerTest {

    @Test
    void deveConfirmarOffsetSomenteDepoisDoProcessamento() throws Exception {

        OcupacaoService service = mock(OcupacaoService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        JsonMapper mapper = JsonMapper.builder().build();

        AgendamentoConfirmadoListener listener =
                new AgendamentoConfirmadoListener(service, mapper);

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

        ConsumerRecord<String, String> registro =
                new ConsumerRecord<>(
                        "salao.agendamento-confirmado",
                        0,
                        0L,
                        "AG-001",
                        json
                );

        listener.receber(registro, acknowledgment);

        ArgumentCaptor<AgendamentoConfirmadoEvent> captor =
                ArgumentCaptor.forClass(
                        AgendamentoConfirmadoEvent.class
                );

        InOrder ordem = inOrder(service, acknowledgment);

        ordem.verify(service)
                .processar(captor.capture());

        ordem.verify(acknowledgment)
                .acknowledge();

        AgendamentoConfirmadoEvent evento =
                captor.getValue();

        assertEquals("EVT-001", evento.getEventoId());
        assertEquals("AG-001", evento.getAgendamentoId());
        assertEquals("PROF-018", evento.getProfissionalId());
    }

    @Test
    void naoDeveConfirmarOffsetQuandoProcessamentoFalhar() {

        OcupacaoService service = mock(OcupacaoService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        JsonMapper mapper = JsonMapper.builder().build();

        AgendamentoConfirmadoListener listener =
                new AgendamentoConfirmadoListener(service, mapper);

        String json = """
                {
                    "eventoId": "EVT-001",
                    "agendamentoId": "AG-001",
                    "profissionalId": "PROF-018",
                    "inicioEm": "2026-08-20T14:00:00-03:00"
                }
                """;

        ConsumerRecord<String, String> registro =
                new ConsumerRecord<>(
                        "salao.agendamento-confirmado",
                        0,
                        0L,
                        "AG-001",
                        json
                );

        doThrow(new RuntimeException("Falha no processamento"))
                .when(service)
                .processar(any(AgendamentoConfirmadoEvent.class));

        assertThrows(
                RuntimeException.class,
                () -> listener.receber(registro, acknowledgment)
        );

        verify(acknowledgment, never())
                .acknowledge();
    }
}