package br.pucminas.aed.equipe07.ocupacao.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class DlqReprocessamentoService {

    private static final String TOPICO_ORIGINAL = "salao.agendamento-confirmado";
    private static final Duration ORCAMENTO_TOTAL = Duration.ofSeconds(5);
    private static final Duration TIMEOUT_POR_POLL = Duration.ofSeconds(1);

    private final ConsumerFactory<Object, Object> consumerFactory;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public DlqReprocessamentoService(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> kafkaTemplate) {

        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    public int reprocessar(String groupIdOriginal) {

        String topicoDlq = TOPICO_ORIGINAL + ".dlq." + groupIdOriginal;
        String grupoReprocessador = groupIdOriginal + "-dlq-reprocessador";

        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Consumer<Object, Object> consumer =
                consumerFactory.createConsumer(grupoReprocessador, null, null, overrides);

        try {
            consumer.subscribe(List.of(topicoDlq));

            List<ConsumerRecord<Object, Object>> registros = coletarRegistros(consumer);

            for (ConsumerRecord<Object, Object> registro : registros) {
                republicar(registro);
            }

            consumer.commitSync();

            return registros.size();

        } finally {
            consumer.close();
        }
    }

    private List<ConsumerRecord<Object, Object>> coletarRegistros(Consumer<Object, Object> consumer) {

        List<ConsumerRecord<Object, Object>> registros = new ArrayList<>();
        long fimDoOrcamento = System.currentTimeMillis() + ORCAMENTO_TOTAL.toMillis();

        while (System.currentTimeMillis() < fimDoOrcamento) {

            ConsumerRecords<Object, Object> lote = consumer.poll(TIMEOUT_POR_POLL);

            if (lote.isEmpty() && !registros.isEmpty()) {
                break;
            }

            lote.forEach(registros::add);
        }

        return registros;
    }

    private void republicar(ConsumerRecord<Object, Object> registro) {

        try {
            ProducerRecord<Object, Object> republicacao =
                new ProducerRecord<>(
                    TOPICO_ORIGINAL,
                    null,
                    registro.timestamp(),
                    registro.key(),
                    registro.value(),
                    copiarHeaders(registro)
                );

            kafkaTemplate
                .send(republicacao)
                    .get(5, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Reprocessamento interrompido", e);

        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Falha ao republicar mensagem da DLQ", e);
        }
    }

    private RecordHeaders copiarHeaders(ConsumerRecord<Object, Object> registro) {

        RecordHeaders headers = new RecordHeaders();

        for (Header header : registro.headers()) {
            headers.add(header);
        }

        return headers;
    }
}
