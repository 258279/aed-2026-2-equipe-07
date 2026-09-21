package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoCanceladoPorConflitoEvent;
import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

@Service
public class OcupacaoService {

    private final OcupacaoRepository repository;
    private final CompensacaoService compensacaoService;

    public OcupacaoService(
            OcupacaoRepository repository,
            CompensacaoService compensacaoService) {

        this.repository = repository;
        this.compensacaoService = compensacaoService;
    }

    @Transactional
    public void processar(AgendamentoConfirmadoEvent evento) {

        validarEvento(evento);

        if (repository.eventoJaProcessado(evento.getEventoId())) {
            return;
        }

        if (repository.existeConflitoDeHorario(
                evento.getProfissionalId(),
                evento.getInicioEm(),
                evento.getAgendamentoId())) {

            publicarCompensacaoDeConflito(evento);
            repository.registrarEventoProcessado(evento.getEventoId());
            return;
        }

        repository.registrarEventoProcessado(
                evento.getEventoId()
        );

        repository.registrarOcupacao(
                evento.getAgendamentoId(),
                evento.getProfissionalId(),
                evento.getInicioEm()
        );
    }

    private void publicarCompensacaoDeConflito(
            AgendamentoConfirmadoEvent evento) {

        AgendamentoCanceladoPorConflitoEvent compensacao =
                new AgendamentoCanceladoPorConflitoEvent(
                        "COMP-" + evento.getEventoId(),
                        evento.getAgendamentoId(),
                        evento.getProfissionalId(),
                        evento.getInicioEm(),
                        "Conflito de horario para o profissional no instante solicitado",
                        OffsetDateTime.now().toString()
                );

        compensacaoService.publicarCancelamentoPorConflito(compensacao);
    }

    private void validarEvento(AgendamentoConfirmadoEvent evento) {

        if (evento.getEventoId() == null || evento.getEventoId().isBlank()) {
            throw new IllegalArgumentException("eventoId obrigatorio");
        }

        if (evento.getAgendamentoId() == null || evento.getAgendamentoId().isBlank()) {
            throw new IllegalArgumentException("agendamentoId obrigatorio");
        }

        if (evento.getProfissionalId() == null || evento.getProfissionalId().isBlank()) {
            throw new IllegalArgumentException("profissionalId obrigatorio");
        }

        if (evento.getInicioEm() == null || evento.getInicioEm().isBlank()) {
            throw new IllegalArgumentException("inicioEm obrigatorio");
        }
    }
}