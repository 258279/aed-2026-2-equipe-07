package br.pucminas.aed.equipe07.agendamentos.domain;

public final class EventoAgendamento {

    private final String agendamentoId;
    private final int versao;
    private final String tipoEvento;
    private final String payload;
    private final String ocorridoEm;

    public EventoAgendamento(
            String agendamentoId,
            int versao,
            String tipoEvento,
            String payload,
            String ocorridoEm) {

        this.agendamentoId = agendamentoId;
        this.versao = versao;
        this.tipoEvento = tipoEvento;
        this.payload = payload;
        this.ocorridoEm = ocorridoEm;
    }

    public String getAgendamentoId() {
        return agendamentoId;
    }

    public int getVersao() {
        return versao;
    }

    public String getTipoEvento() {
        return tipoEvento;
    }

    public String getPayload() {
        return payload;
    }

    public String getOcorridoEm() {
        return ocorridoEm;
    }
}
