package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEventDetalhado;
import br.pucminas.aed.equipe07.ocupacao.service.AgendamentoConfirmadoJanelaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AgendamentoConfirmadoJanelaListenerTest {

    @Test
    void deveUsarGrupoProprio() throws Exception {

        Method metodo = AgendamentoConfirmadoJanelaListener.class.getMethod(
                "receber",
                ConsumerRecord.class,
                Acknowledgment.class
        );

        KafkaListener annotation = metodo.getAnnotation(KafkaListener.class);

        assertEquals(
                "servico-ocupacao-agregacao-janelas",
                annotation.groupId()
        );
    }

    @Test
    void deveDeserializarEventoCompletoEConfirmarOffsetDepoisDaAgregacao() throws Exception {

        AgendamentoConfirmadoJanelaService service =
                mock(AgendamentoConfirmadoJanelaService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        JsonMapper mapper = JsonMapper.builder().build();

        AgendamentoConfirmadoJanelaListener listener =
                new AgendamentoConfirmadoJanelaListener(service, mapper);

        String json = """
                {
                    "eventoId": "EVT-002",
                    "agendamentoId": "AG-002",
                    "profissionalId": "PROF-019",
                    "servicoId": "SERV-010",
                    "inicioEm": "2026-08-22T14:00:00-03:00",
                    "prioridade": "URGENTE",
                    "ocorridoEm": "2026-08-16T12:37:00-03:00"
                }
                """;

        ConsumerRecord<String, String> registro =
                new ConsumerRecord<>(
                        "salao.agendamento-confirmado",
                        0,
                        0L,
                        "AG-002",
                        json
                );

        listener.receber(registro, acknowledgment);

        inOrder(service, acknowledgment)
                .verify(service)
                .processar(any(AgendamentoConfirmadoEventDetalhado.class));

        verify(acknowledgment).acknowledge();
    }
}