package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OcupacaoService {

    private final OcupacaoRepository repository;

    public OcupacaoService(OcupacaoRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void processar(AgendamentoConfirmadoEvent evento) {

        if (repository.eventoJaProcessado(evento.getEventoId())) {
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
}