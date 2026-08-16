package br.pucminas.aed.equipe07.ocupacao.domain;

public final class AgendamentoConfirmadoEvent {

    private final String eventoId;
    private final String agendamentoId;
    private final String profissionalId;
    private final String inicioEm;

    public AgendamentoConfirmadoEvent(
            String eventoId,
            String agendamentoId,
            String profissionalId,
            String inicioEm) {

        this.eventoId = eventoId;
        this.agendamentoId = agendamentoId;
        this.profissionalId = profissionalId;
        this.inicioEm = inicioEm;
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

    public String getInicioEm() {
        return inicioEm;
    }
}