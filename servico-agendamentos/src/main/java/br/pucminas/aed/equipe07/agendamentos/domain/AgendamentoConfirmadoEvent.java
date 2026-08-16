package br.pucminas.aed.equipe07.agendamentos.domain;

public final class AgendamentoConfirmadoEvent {

    private final String eventoId;
    private final String agendamentoId;
    private final String profissionalId;
    private final String servicoId;
    private final String inicioEm;
    private final String prioridade;
    private final String ocorridoEm;

    public AgendamentoConfirmadoEvent(
            String eventoId,
            String agendamentoId,
            String profissionalId,
            String servicoId,
            String inicioEm,
            String prioridade,
            String ocorridoEm) {

        this.eventoId = eventoId;
        this.agendamentoId = agendamentoId;
        this.profissionalId = profissionalId;
        this.servicoId = servicoId;
        this.inicioEm = inicioEm;
        this.prioridade = prioridade;
        this.ocorridoEm = ocorridoEm;
    }

    public String getEventoId() {
        return eventoId;
    }

    public String getAgendamentoId() {
        return agendamentoId;
    }

    public String getProfissionalId() {
        return profissionalId;
    }

    public String getServicoId() {
        return servicoId;
    }

    public String getInicioEm() {
        return inicioEm;
    }

    public String getPrioridade() {
        return prioridade;
    }

    public String getOcorridoEm() {
        return ocorridoEm;
    }
}
