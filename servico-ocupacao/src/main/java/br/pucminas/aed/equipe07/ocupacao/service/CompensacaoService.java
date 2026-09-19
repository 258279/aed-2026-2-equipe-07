package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoCanceladoPorConflitoEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

@Service
public class CompensacaoService {

    private static final String TOPICO_COMPENSACAO = "salao.agendamento-cancelado-por-conflito";

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final JsonMapper jsonMapper;

    public CompensacaoService(
            KafkaTemplate<Object, Object> kafkaTemplate,
            JsonMapper jsonMapper) {

        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
    }

    public void publicarCancelamentoPorConflito(
            AgendamentoCanceladoPorConflitoEvent evento) {

        try {
            String payload = jsonMapper.writeValueAsString(evento);

            RecordHeaders headers = new RecordHeaders();
            adicionarHeader(headers, "ce_specversion", "1.0");
            adicionarHeader(headers, "ce_id", evento.getEventoId());
            adicionarHeader(headers, "ce_source", "/salao/servico-ocupacao");
            adicionarHeader(headers, "ce_type", "salao.agendamento.cancelado-por-conflito.v1");
            adicionarHeader(headers, "ce_time", evento.getOcorridoEm());

            ProducerRecord<Object, Object> registro =
                    new ProducerRecord<>(
                            TOPICO_COMPENSACAO,
                            null,
                            evento.getAgendamentoId(),
                            payload,
                            headers
                    );

            kafkaTemplate.send(registro).join();

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao publicar evento de compensacao", e);
        }
    }

    private void adicionarHeader(
            RecordHeaders headers,
            String nome,
            String valor) {

        headers.add(nome, valor.getBytes(StandardCharsets.UTF_8));
    }
}