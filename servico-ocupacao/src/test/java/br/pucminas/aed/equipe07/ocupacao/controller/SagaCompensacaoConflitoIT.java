package br.pucminas.aed.equipe07.ocupacao.controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "salao.agendamento-confirmado",
                "salao.agendamento-cancelado-por-conflito"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class SagaCompensacaoConflitoIT {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private JdbcTemplate banco;

    @BeforeEach
    void preparar() {

        registry.getListenerContainers().forEach(
                container -> ContainerTestUtils.waitForAssignment(container, 1));

        banco.execute("""
                CREATE TABLE IF NOT EXISTS eventos_processados (
                    evento_id VARCHAR(100) PRIMARY KEY,
                    processado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """);

        banco.execute("""
                CREATE TABLE IF NOT EXISTS projecao_ocupacao (
                    agendamento_id VARCHAR(100) PRIMARY KEY,
                    profissional_id VARCHAR(100) NOT NULL,
                    inicio_em VARCHAR(40) NOT NULL
                )
                """);

        banco.update("DELETE FROM eventos_processados");
        banco.update("DELETE FROM projecao_ocupacao");
    }

    @Test
    void devePublicarEventoDeCompensacaoQuandoHorarioJaEstiverOcupado() {

        String primeiro = """
                {"eventoId":"EVT-SAGA-001","agendamentoId":"AG-SAGA-001","profissionalId":"PROF-777","inicioEm":"2026-09-22T10:00:00-03:00"}
                """;

        String segundo = """
                {"eventoId":"EVT-SAGA-002","agendamentoId":"AG-SAGA-002","profissionalId":"PROF-777","inicioEm":"2026-09-22T10:00:00-03:00"}
                """;

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-SAGA-001", primeiro).join();

        aguardarOcupacao("AG-SAGA-001", Duration.ofSeconds(10));

        kafkaTemplate.send("salao.agendamento-confirmado", "AG-SAGA-002", segundo).join();

        Consumer<String, String> consumidor =
                criarConsumidor("teste-saga-compensacao", "salao.agendamento-cancelado-por-conflito");

        ConsumerRecord<String, String> compensacao = KafkaTestUtils.getSingleRecord(
                consumidor,
                "salao.agendamento-cancelado-por-conflito",
                Duration.ofSeconds(10)
        );

        assertTrue(compensacao.value().contains("\"agendamentoId\":\"AG-SAGA-002\""));
        assertTrue(compensacao.value().contains("\"motivo\":\"Conflito de horario"));

        Integer totalOcupacoes = banco.queryForObject(
                "SELECT COUNT(*) FROM projecao_ocupacao WHERE profissional_id = ? AND inicio_em = ?",
                Integer.class,
                "PROF-777",
                "2026-09-22T10:00:00-03:00"
        );

        assertEquals(1, totalOcupacoes);

        consumidor.close();
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

    private void aguardarOcupacao(String agendamentoId, Duration timeout) {

        long limite = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < limite) {

            Integer quantidade = banco.queryForObject(
                    "SELECT COUNT(*) FROM projecao_ocupacao WHERE agendamento_id = ?",
                    Integer.class,
                    agendamentoId
            );

            if (quantidade != null && quantidade > 0) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Teste interrompido", e);
            }
        }

        throw new AssertionError("Timeout aguardando ocupacao do agendamento " + agendamentoId);
    }
}