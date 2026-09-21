package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.service.AgendamentoConfirmadoJanelaService;
import br.pucminas.aed.equipe07.ocupacao.service.OcupacaoService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
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
class AgendamentoConfirmadoListenerDlqIT {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private OcupacaoService ocupacaoService;

    @BeforeEach
    void aguardarContainersProntos() {
        registry.getListenerContainers().forEach(
                container -> ContainerTestUtils.waitForAssignment(container, 1));
    }

    @TestConfiguration
    static class MocksConfig {

        @Bean
        @Primary
        OcupacaoService ocupacaoService() {
            OcupacaoService mock = mock(OcupacaoService.class);
            doThrow(new RuntimeException("falha simulada"))
                    .when(mock)
                    .processar(any());
            return mock;
        }

        @Bean
        @Primary
        AgendamentoConfirmadoJanelaService agendamentoConfirmadoJanelaService() {
            return mock(AgendamentoConfirmadoJanelaService.class);
        }
    }

    @Test
    void mensagemComFalhaPersistenteDeveIrParaDlqDepoisDeTresTentativas() {

        String json = """
                {"eventoId":"EVT-DLQ-001","agendamentoId":"AG-DLQ-001","profissionalId":"PROF-001","inicioEm":"2026-09-01T10:00:00-03:00"}""";

        ProducerRecord<Object, Object> registroOriginal =
                new ProducerRecord<>("salao.agendamento-confirmado", "AG-DLQ-001", json);
        registroOriginal.headers().add("ce_id", "EVT-DLQ-001".getBytes(StandardCharsets.UTF_8));
        registroOriginal.headers().add("ce_type", "salao.agendamento.confirmado.v1".getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(registroOriginal);

        Consumer<String, String> consumidorDlq =
                criarConsumidor("teste-dlq-ocupacao", "salao.agendamento-confirmado.dlq.servico-ocupacao");

        ConsumerRecord<String, String> registro = KafkaTestUtils.getSingleRecord(
                consumidorDlq,
                "salao.agendamento-confirmado.dlq.servico-ocupacao",
                Duration.ofSeconds(15)
        );

        assertNotNull(registro);
        assertEquals(json, registro.value());
        assertEquals("EVT-DLQ-001", headerAsString(registro, "ce_id"));
        assertNotNull(registro.headers().lastHeader("kafka_dlt-exception-message"));

        Mockito.verify(ocupacaoService, Mockito.times(4)).processar(Mockito.any());

        consumidorDlq.close();
    }

    @Test
    void mensagemComFalhaTransitoriaDeveSerProcessadaDentroDoOrcamentoDeRetry() {

        Mockito.doThrow(new RuntimeException("falha transitoria 1"))
                .doThrow(new RuntimeException("falha transitoria 2"))
                .doNothing()
                .when(ocupacaoService)
                .processar(any());

        String json = """
                {"eventoId":"EVT-RETRY-001","agendamentoId":"AG-RETRY-001","profissionalId":"PROF-001","inicioEm":"2026-09-01T10:00:00-03:00"}""";

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-RETRY-001", json);

        Mockito.verify(ocupacaoService, Mockito.timeout(15000).times(3)).processar(any());

        Consumer<String, String> consumidorDlq =
                criarConsumidor("teste-dlq-ocupacao-retry-sucesso", "salao.agendamento-confirmado.dlq.servico-ocupacao");

        ConsumerRecords<String, String> registrosDlq =
                KafkaTestUtils.getRecords(consumidorDlq, Duration.ofSeconds(2));

        assertTrue(registrosDlq.isEmpty(),
                "mensagem recuperada dentro do orcamento de retry nao deveria ir para a DLQ");

        consumidorDlq.close();
    }

    @Test
    void mensagemComJsonInvalidoDeveIrParaDlqSemEsperarRetry() {

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-POISON-001", "isto nao e json");

        Consumer<String, String> consumidorDlq =
                criarConsumidor("teste-dlq-ocupacao-poison", "salao.agendamento-confirmado.dlq.servico-ocupacao");

        ConsumerRecord<String, String> registro = KafkaTestUtils.getSingleRecord(
                consumidorDlq,
                "salao.agendamento-confirmado.dlq.servico-ocupacao",
                Duration.ofSeconds(5)
        );

        assertNotNull(registro);
        assertEquals("isto nao e json", registro.value());

        consumidorDlq.close();
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
