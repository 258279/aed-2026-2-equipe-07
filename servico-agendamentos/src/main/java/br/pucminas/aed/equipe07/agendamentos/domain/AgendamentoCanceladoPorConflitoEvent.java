package br.pucminas.aed.equipe07.agendamentos.domain;

public final class AgendamentoCanceladoPorConflitoEvent {

    private final String eventoId;
    private final String agendamentoId;
    private final String profissionalId;
    private final String inicioEm;
    private final String motivo;
    private final String ocorridoEm;

    public AgendamentoCanceladoPorConflitoEvent(
            String eventoId,
            String agendamentoId,
            String profissionalId,
            String inicioEm,
            String motivo,
            String ocorridoEm) {

        this.eventoId = eventoId;
        this.agendamentoId = agendamentoId;
        this.profissionalId = profissionalId;
        this.inicioEm = inicioEm;
        this.motivo = motivo;
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

    public String getInicioEm() {
        return inicioEm;
    }

    public String getMotivo() {
        return motivo;
    }

    public String getOcorridoEm() {
        return ocorridoEm;
    }
}