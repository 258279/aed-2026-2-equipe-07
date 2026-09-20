package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

@Service
public class AgendamentoCicloDeVidaService {

    private final AgendamentoEventStore eventStore;
    private final ProjetorFaltasEstornos projetor;

    public AgendamentoCicloDeVidaService(
            AgendamentoEventStore eventStore,
            ProjetorFaltasEstornos projetor) {

        this.eventStore = eventStore;
        this.projetor = projetor;
    }

    public void cancelar(String agendamentoId) {

        Agendamento agendamento = carregarAgregado(agendamentoId);

        List<EventoAgendamento> novosEventos =
                agendamento.cancelar(OffsetDateTime.now().toString());

        eventStore.gravar(novosEventos);
        projetor.processar(novosEventos);
    }

    public void marcarNaoComparecimento(String agendamentoId) {

        Agendamento agendamento = carregarAgregado(agendamentoId);

        List<EventoAgendamento> novosEventos =
                agendamento.marcarNaoComparecimento(OffsetDateTime.now().toString());

        eventStore.gravar(novosEventos);
        projetor.processar(novosEventos);
    }

    private Agendamento carregarAgregado(String agendamentoId) {
        return Agendamento.reconstruir(eventStore.carregarStream(agendamentoId));
    }
}
