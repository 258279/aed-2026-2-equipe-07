package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class AgendamentoService {

    private static final String TOPICO = "salao.agendamento-confirmado";

    private final KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker;
    private final AgendamentoEventStore eventStore;
    private final AgendamentoStatusService statusService;
    private final JsonMapper jsonMapper;

    public AgendamentoService(
            KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker,
            AgendamentoEventStore eventStore,
            AgendamentoStatusService statusService,
            JsonMapper jsonMapper) {

        this.clienteDoBroker = clienteDoBroker;
        this.eventStore = eventStore;
        this.statusService = statusService;
        this.jsonMapper = jsonMapper;
    }

    public CompletableFuture<SendResult<String, AgendamentoConfirmadoEvent>>
    publicarConfirmacao(AgendamentoConfirmadoEvent evento) {

        eventStore.gravar(List.of(
                new EventoAgendamento(
                        evento.getAgendamentoId(),
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        jsonMapper.writeValueAsString(evento),
                        evento.getOcorridoEm()
                )
        ));

        statusService.marcarConfirmado(evento.getAgendamentoId());

        RecordHeaders headers = new RecordHeaders();

        adicionarHeader(headers, "ce_specversion", "1.0");
        adicionarHeader(headers, "ce_id", evento.getEventoId());
        adicionarHeader(
                headers,
                "ce_source",
                "/salao/servico-agendamentos"
        );
        adicionarHeader(
                headers,
                "ce_type",
                "salao.agendamento.confirmado.v1"
        );
        adicionarHeader(
                headers,
                "ce_time",
                evento.getOcorridoEm()
        );

        ProducerRecord<String, AgendamentoConfirmadoEvent> registro =
                new ProducerRecord<>(
                        TOPICO,
                        null,
                        evento.getAgendamentoId(),
                        evento,
                        headers
                );

        CompletableFuture<SendResult<String, AgendamentoConfirmadoEvent>> resultado =
                clienteDoBroker.send(registro);

        resultado.whenComplete(this::tratarResultadoDaPublicacao);

        return resultado;
    }

    private void adicionarHeader(
            RecordHeaders headers,
            String nome,
            String valor) {

        headers.add(
                nome,
                valor.getBytes(StandardCharsets.UTF_8)
        );
    }

    private void tratarResultadoDaPublicacao(
            SendResult<String, AgendamentoConfirmadoEvent> resultado,
            Throwable erro) {

        if (erro != null) {
            System.err.println(
                    "Falha ao publicar AgendamentoConfirmadoEvent: "
                            + erro.getMessage()
            );
        }
    }
}