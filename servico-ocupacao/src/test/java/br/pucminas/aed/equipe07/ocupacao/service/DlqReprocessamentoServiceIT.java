package br.pucminas.aed.equipe07.ocupacao.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "salao.agendamento-confirmado",
                "salao.agendamento-confirmado.dlq.servico-ocupacao"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DlqReprocessamentoServiceIT {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private DlqReprocessamentoService service;

    @TestConfiguration
    static class MocksConfig {

        @Bean
        @Primary
        OcupacaoService ocupacaoService() {
            return mock(OcupacaoService.class);
        }

        @Bean
        @Primary
        AgendamentoConfirmadoJanelaService agendamentoConfirmadoJanelaService() {
            return mock(AgendamentoConfirmadoJanelaService.class);
        }
    }

    @Test
    void deveMoverMensagemDaDlqDeVoltaParaOTopicoOriginal() throws InterruptedException {

        String json = "{\"eventoId\":\"EVT-REPROC-001\"}";

        ProducerRecord<Object, Object> registroDlq =
            new ProducerRecord<>(
                "salao.agendamento-confirmado.dlq.servico-ocupacao",
                "AG-REPROC-001",
                json
            );
        registroDlq.headers().add("ce_id", "EVT-REPROC-001".getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(registroDlq).join();

        int reprocessados = service.reprocessar("servico-ocupacao");

        assertEquals(1, reprocessados);

        Consumer<String, String> consumidorOriginal =
                criarConsumidor("teste-reproc-original", "salao.agendamento-confirmado");

        ConsumerRecord<String, String> registro = KafkaTestUtils.getSingleRecord(
                consumidorOriginal,
                "salao.agendamento-confirmado",
                Duration.ofSeconds(10)
        );

        assertNotNull(registro);
        assertEquals(json, registro.value());
        assertEquals("EVT-REPROC-001", headerAsString(registro, "ce_id"));

        consumidorOriginal.close();
    }

    @Test
    void reinvocarSemNovasMensagensDeveRetornarZero() {

        service.reprocessar("servico-ocupacao");

        int segundaChamada = service.reprocessar("servico-ocupacao");

        assertEquals(0, segundaChamada);
    }

    private Consumer<String, String> criarConsumidor(String grupo, String topico) {

        Map<String, Object> props = KafkaTestUtils.consumerProps(grupo, "true", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();

        broker.consumeFromAnEmbeddedTopic(consumer, topico);

        return consumer;
    }

    private String headerAsString(ConsumerRecord<String, String> registro, String nome) {
        return new String(registro.headers().lastHeader(nome).value(), StandardCharsets.UTF_8);
    }
}
