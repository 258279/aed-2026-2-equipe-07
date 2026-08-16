package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.ocupacao.service.OcupacaoService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class AgendamentoConfirmadoListener {

    private final OcupacaoService service;
    private final JsonMapper conversor;

    public AgendamentoConfirmadoListener(
            OcupacaoService service,
            JsonMapper conversor) {

        this.service = service;
        this.conversor = conversor;
    }

    @KafkaListener(
            topics = "salao.agendamento-confirmado",
            groupId = "servico-ocupacao",
            ackMode = "MANUAL_IMMEDIATE"
    )
    public void receber(
            ConsumerRecord<String, String> registro,
            Acknowledgment acknowledgment) throws Exception {

        AgendamentoConfirmadoEvent evento =
                conversor.readValue(
                        registro.value(),
                        AgendamentoConfirmadoEvent.class
                );

        service.processar(evento);

        acknowledgment.acknowledge();
    }
}