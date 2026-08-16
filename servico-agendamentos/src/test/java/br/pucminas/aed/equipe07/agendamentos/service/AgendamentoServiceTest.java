package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AgendamentoServiceTest {

    @Test
    void devePublicarAgendamentoConfirmadoComCloudEventsEChaveDoAgendamento() {

        KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker =
                mock(KafkaTemplate.class);

        when(clienteDoBroker.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        AgendamentoService service =
                new AgendamentoService(clienteDoBroker);

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "SERV-004",
                        "2026-08-20T14:00:00-03:00",
                        "PADRAO",
                        "2026-08-16T12:30:00-03:00"
                );

        service.publicarConfirmacao(evento);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, AgendamentoConfirmadoEvent>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        verify(clienteDoBroker).send(captor.capture());

        ProducerRecord<String, AgendamentoConfirmadoEvent> registro =
                captor.getValue();

        assertEquals(
                "salao.agendamento-confirmado",
                registro.topic()
        );

        assertEquals(
                "AG-001",
                registro.key()
        );

        assertEquals(
                evento,
                registro.value()
        );

        assertEquals(
                "1.0",
                valorDoHeader(registro, "ce_specversion")
        );

        assertEquals(
                "EVT-001",
                valorDoHeader(registro, "ce_id")
        );

        assertEquals(
                "/salao/servico-agendamentos",
                valorDoHeader(registro, "ce_source")
        );

        assertEquals(
                "salao.agendamento.confirmado.v1",
                valorDoHeader(registro, "ce_type")
        );

        assertEquals(
                "2026-08-16T12:30:00-03:00",
                valorDoHeader(registro, "ce_time")
        );
    }

    private String valorDoHeader(
            ProducerRecord<String, AgendamentoConfirmadoEvent> registro,
            String nome) {

        Header header = registro.headers().lastHeader(nome);

        assertNotNull(header);

        return new String(
                header.value(),
                StandardCharsets.UTF_8
        );
    }
}