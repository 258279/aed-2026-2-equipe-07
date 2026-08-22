package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEventDetalhado;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class AgendamentoConfirmadoJanelaService {

    private static final int TAMANHO_DA_JANELA_EM_MINUTOS = 15;
    private static final DateTimeFormatter FORMATADOR_ISO =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final AgendamentoConfirmadoJanelaRepository repository;

    public AgendamentoConfirmadoJanelaService(
            AgendamentoConfirmadoJanelaRepository repository) {

        this.repository = repository;
    }

        @Transactional
    public void processar(AgendamentoConfirmadoEventDetalhado evento) {

        OffsetDateTime ocorridoEm = OffsetDateTime.parse(evento.getOcorridoEm());
        OffsetDateTime inicioDaJanela = inicioDaJanela(ocorridoEm);
        OffsetDateTime fimDaJanela = inicioDaJanela.plusMinutes(TAMANHO_DA_JANELA_EM_MINUTOS);

        int quantidadeAtualizada = repository.registrarConfirmacao(
                inicioDaJanela.format(FORMATADOR_ISO),
                fimDaJanela.format(FORMATADOR_ISO),
                evento.getPrioridade()
        );

        System.out.println(
                "Confirmações na janela "
                        + inicioDaJanela.format(FORMATADOR_ISO)
                        + " - "
                        + fimDaJanela.format(FORMATADOR_ISO)
                        + " | prioridade="
                        + evento.getPrioridade()
                        + " | total="
                        + quantidadeAtualizada
        );
    }

    private OffsetDateTime inicioDaJanela(OffsetDateTime ocorridoEm) {

        int minutoInicial =
                (ocorridoEm.getMinute() / TAMANHO_DA_JANELA_EM_MINUTOS)
                        * TAMANHO_DA_JANELA_EM_MINUTOS;

        return ocorridoEm
                .withMinute(minutoInicial)
                .withSecond(0)
                .withNano(0);
    }
}