package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Service
public class AgendamentoService {

    private static final String TOPICO = "salao.agendamento-confirmado";

    private final KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker;

    public AgendamentoService(
            KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker) {

        this.clienteDoBroker = clienteDoBroker;
    }

    public CompletableFuture<SendResult<String, AgendamentoConfirmadoEvent>>
    publicarConfirmacao(AgendamentoConfirmadoEvent evento) {

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