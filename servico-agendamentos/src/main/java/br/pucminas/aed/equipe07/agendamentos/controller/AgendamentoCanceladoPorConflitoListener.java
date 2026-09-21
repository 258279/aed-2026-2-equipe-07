package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoCanceladoPorConflitoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.CompensacaoReacaoService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class AgendamentoCanceladoPorConflitoListener {

    private final CompensacaoReacaoService service;
    private final JsonMapper conversor;

    public AgendamentoCanceladoPorConflitoListener(
            CompensacaoReacaoService service,
            JsonMapper conversor) {

        this.service = service;
        this.conversor = conversor;
    }

    @KafkaListener(
            topics = "salao.agendamento-cancelado-por-conflito",
            groupId = "servico-agendamentos-compensacao",
            containerFactory = "compensacaoContainerFactory",
            ackMode = "MANUAL_IMMEDIATE"
    )
    public void receber(
            ConsumerRecord<String, String> registro,
            Acknowledgment acknowledgment) throws Exception {

        AgendamentoCanceladoPorConflitoEvent evento =
                conversor.readValue(
                        registro.value(),
                        AgendamentoCanceladoPorConflitoEvent.class
                );

        service.processar(evento);

        acknowledgment.acknowledge();
    }
}