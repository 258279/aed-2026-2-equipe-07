package br.pucminas.aed.equipe07.agendamentos.domain;

import java.util.List;

public final class Agendamento {

    public static final String TIPO_CONFIRMADO = "AgendamentoConfirmado";
    public static final String TIPO_CANCELADO = "AgendamentoCancelado";
    public static final String TIPO_SINAL_ESTORNADO = "SinalEstornado";
    public static final String TIPO_NAO_COMPARECEU = "AgendamentoNaoCompareceu";
    public static final String TIPO_MULTA_COBRADA = "MultaCobrada";

    public enum Status { CONFIRMADO, CANCELADO, NAO_COMPARECEU }

    private final String agendamentoId;
    private final Status status;
    private final int versaoAtual;

    private Agendamento(String agendamentoId, Status status, int versaoAtual) {
        this.agendamentoId = agendamentoId;
        this.status = status;
        this.versaoAtual = versaoAtual;
    }

    public static Agendamento reconstruir(List<EventoAgendamento> eventos) {

        if (eventos.isEmpty()) {
            throw new IllegalStateException("Nenhum evento encontrado para este agendamento");
        }

        String agendamentoId = eventos.get(0).getAgendamentoId();
        Status status = null;
        int versaoAtual = 0;

        for (EventoAgendamento evento : eventos) {
            status = aplicar(status, evento.getTipoEvento());
            versaoAtual = evento.getVersao();
        }

        return new Agendamento(agendamentoId, status, versaoAtual);
    }

    private static Status aplicar(Status statusAtual, String tipoEvento) {

        return switch (tipoEvento) {
            case TIPO_CONFIRMADO -> Status.CONFIRMADO;
            case TIPO_CANCELADO -> Status.CANCELADO;
            case TIPO_NAO_COMPARECEU -> Status.NAO_COMPARECEU;
            case TIPO_SINAL_ESTORNADO, TIPO_MULTA_COBRADA -> statusAtual;
            default -> throw new IllegalStateException("Tipo de evento desconhecido: " + tipoEvento);
        };
    }

    public List<EventoAgendamento> cancelar(String ocorridoEm) {

        if (status != Status.CONFIRMADO) {
            throw new IllegalStateException(
                    "Só é possível cancelar um agendamento confirmado. Status atual: " + status);
        }

        return List.of(
                new EventoAgendamento(agendamentoId, versaoAtual + 1, TIPO_CANCELADO, "{}", ocorridoEm),
                new EventoAgendamento(agendamentoId, versaoAtual + 2, TIPO_SINAL_ESTORNADO, "{}", ocorridoEm)
        );
    }

    public List<EventoAgendamento> marcarNaoComparecimento(String ocorridoEm) {

        if (status != Status.CONFIRMADO) {
            throw new IllegalStateException(
                    "Só é possível marcar não comparecimento de um agendamento confirmado. Status atual: " + status);
        }

        return List.of(
                new EventoAgendamento(agendamentoId, versaoAtual + 1, TIPO_NAO_COMPARECEU, "{}", ocorridoEm),
                new EventoAgendamento(agendamentoId, versaoAtual + 2, TIPO_MULTA_COBRADA, "{}", ocorridoEm)
        );
    }

    public String getAgendamentoId() {
        return agendamentoId;
    }

    public Status getStatus() {
        return status;
    }

    public int getVersaoAtual() {
        return versaoAtual;
    }
}
