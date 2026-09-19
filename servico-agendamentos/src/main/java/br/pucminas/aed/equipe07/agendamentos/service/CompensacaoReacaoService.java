package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoCanceladoPorConflitoEvent;
import org.springframework.stereotype.Service;

@Service
public class CompensacaoReacaoService {

    private final AgendamentoStatusService statusService;

    public CompensacaoReacaoService(AgendamentoStatusService statusService) {
        this.statusService = statusService;
    }

    public void processar(AgendamentoCanceladoPorConflitoEvent evento) {

        validar(evento);

        statusService.aplicarCompensacao(
                evento.getEventoId(),
                evento.getAgendamentoId()
        );
    }

    private void validar(AgendamentoCanceladoPorConflitoEvent evento) {

        if (evento.getEventoId() == null || evento.getEventoId().isBlank()) {
            throw new IllegalArgumentException("eventoId obrigatorio");
        }

        if (evento.getAgendamentoId() == null || evento.getAgendamentoId().isBlank()) {
            throw new IllegalArgumentException("agendamentoId obrigatorio");
        }

        if (evento.getMotivo() == null || evento.getMotivo().isBlank()) {
            throw new IllegalArgumentException("motivo obrigatorio");
        }
    }
}