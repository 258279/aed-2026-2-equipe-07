package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEventDetalhado;
import br.pucminas.aed.equipe07.ocupacao.service.AgendamentoConfirmadoJanelaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class AgendamentoConfirmadoJanelaListener {

    private static final String GRUPO = "servico-ocupacao-agregacao-janelas";

    private final AgendamentoConfirmadoJanelaService service;
    private final JsonMapper conversor;

    public AgendamentoConfirmadoJanelaListener(
            AgendamentoConfirmadoJanelaService service,
            JsonMapper conversor) {

        this.service = service;
        this.conversor = conversor;
    }

    @KafkaListener(
            topics = "salao.agendamento-confirmado",
            groupId = GRUPO,
            ackMode = "MANUAL_IMMEDIATE"
    )
    public void receber(
            ConsumerRecord<String, String> registro,
            Acknowledgment acknowledgment) throws Exception {

        AgendamentoConfirmadoEventDetalhado evento =
                conversor.readValue(
                        registro.value(),
                        AgendamentoConfirmadoEventDetalhado.class
                );

        service.processar(evento);

        acknowledgment.acknowledge();
    }
}