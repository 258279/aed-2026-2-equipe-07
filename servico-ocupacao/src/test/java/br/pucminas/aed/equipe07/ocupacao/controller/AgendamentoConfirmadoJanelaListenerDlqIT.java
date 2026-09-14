package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.service.AgendamentoConfirmadoJanelaService;
import br.pucminas.aed.equipe07.ocupacao.service.OcupacaoService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
                "salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class AgendamentoConfirmadoJanelaListenerDlqIT {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private AgendamentoConfirmadoJanelaService agendamentoConfirmadoJanelaService;

    @BeforeEach
    void aguardarContainersProntos() {
        registry.getListenerContainers().forEach(
                container -> ContainerTestUtils.waitForAssignment(container, 1));
    }

    @TestConfiguration
    static class MocksConfig {

        @Bean
        @Primary
        AgendamentoConfirmadoJanelaService agendamentoConfirmadoJanelaService() {
            AgendamentoConfirmadoJanelaService mock = mock(AgendamentoConfirmadoJanelaService.class);
            doThrow(new RuntimeException("falha simulada"))
                    .when(mock)
                    .processar(any());
            return mock;
        }

        @Bean
        @Primary
        OcupacaoService ocupacaoService() {
            return mock(OcupacaoService.class);
        }
    }

    @Test
    void mensagemComFalhaPersistenteDeveIrParaDlqDepoisDeTresTentativas() {

        String json = """
                {"eventoId":"EVT-DLQ-002","agendamentoId":"AG-DLQ-002","profissionalId":"PROF-002","servicoId":"SERV-001","inicioEm":"2026-09-01T10:00:00-03:00","prioridade":"PADRAO","ocorridoEm":"2026-09-01T09:55:00-03:00"}""";

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-DLQ-002", json);

        Consumer<String, String> consumidorDlq =
                criarConsumidor("teste-dlq-janelas", "salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas");

        ConsumerRecord<String, String> registro = KafkaTestUtils.getSingleRecord(
                consumidorDlq,
                "salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas",
                Duration.ofSeconds(15)
        );

        assertNotNull(registro);
        assertEquals(json, registro.value());

        Mockito.verify(agendamentoConfirmadoJanelaService, Mockito.times(4)).processar(Mockito.any());

        consumidorDlq.close();
    }

    @Test
    void mensagemComFalhaTransitoriaDeveSerProcessadaDentroDoOrcamentoDeRetry() {

        Mockito.doThrow(new RuntimeException("falha transitoria 1"))
                .doThrow(new RuntimeException("falha transitoria 2"))
                .doNothing()
                .when(agendamentoConfirmadoJanelaService)
                .processar(any());

        String json = """
                {"eventoId":"EVT-RETRY-002","agendamentoId":"AG-RETRY-002","profissionalId":"PROF-002","servicoId":"SERV-001","inicioEm":"2026-09-01T10:00:00-03:00","prioridade":"PADRAO","ocorridoEm":"2026-09-01T09:55:00-03:00"}""";

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-RETRY-002", json);

        Mockito.verify(agendamentoConfirmadoJanelaService, Mockito.timeout(15000).times(3)).processar(any());

        Consumer<String, String> consumidorDlq =
                criarConsumidor("teste-dlq-janelas-retry-sucesso", "salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas");

        ConsumerRecords<String, String> registrosDlq =
                KafkaTestUtils.getRecords(consumidorDlq, Duration.ofSeconds(2));

        assertTrue(registrosDlq.isEmpty(),
                "mensagem recuperada dentro do orcamento de retry nao deveria ir para a DLQ");

        consumidorDlq.close();
    }

    @Test
    void mensagemComJsonInvalidoDeveIrParaDlqSemEsperarRetry() {

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-POISON-002", "isto nao e json");

        Consumer<String, String> consumidorDlq =
                criarConsumidor("teste-dlq-janelas-poison", "salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas");

        ConsumerRecord<String, String> registro = KafkaTestUtils.getSingleRecord(
                consumidorDlq,
                "salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas",
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
}
