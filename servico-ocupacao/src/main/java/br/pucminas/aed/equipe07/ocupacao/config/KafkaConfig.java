package br.pucminas.aed.equipe07.ocupacao.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import tools.jackson.core.JacksonException;

@Configuration
public class KafkaConfig {

    private static final String TOPICO_ORIGINAL = "salao.agendamento-confirmado";
    private static final int NUMERO_DE_TENTATIVAS = 3;
    private static final long INTERVALO_INICIAL_MS = 1000L;
    private static final double MULTIPLICADOR = 2.0;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> ocupacaoContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> kafkaTemplate) {

        return criarFactory(consumerFactory, kafkaTemplate, "servico-ocupacao");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> janelaContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> kafkaTemplate) {

        return criarFactory(consumerFactory, kafkaTemplate, "servico-ocupacao-agregacao-janelas");
    }

    private ConcurrentKafkaListenerContainerFactory<Object, Object> criarFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<Object, Object> kafkaTemplate,
            String groupId) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(criarErrorHandler(kafkaTemplate, groupId));

        return factory;
    }

    private DefaultErrorHandler criarErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            String groupId) {

        String topicoDlq = TOPICO_ORIGINAL + ".dlq." + groupId;

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception ex) -> new TopicPartition(topicoDlq, -1)
        );

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(NUMERO_DE_TENTATIVAS);
        backOff.setInitialInterval(INTERVALO_INICIAL_MS);
        backOff.setMultiplier(MULTIPLICADOR);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(JacksonException.class);

        return errorHandler;
    }
}
