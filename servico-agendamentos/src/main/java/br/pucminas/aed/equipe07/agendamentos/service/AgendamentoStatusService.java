package br.pucminas.aed.equipe07.agendamentos.service;

import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AgendamentoStatusService {

    private static final String STATUS_DESCONHECIDO = "DESCONHECIDO";
    private static final String STATUS_CONFIRMADO = "CONFIRMADO";
    private static final String STATUS_CANCELADO_POR_CONFLITO = "CANCELADO_POR_CONFLITO";

    private final ConcurrentHashMap<String, String> statusPorAgendamento = new ConcurrentHashMap<>();
    private final Set<String> eventosCompensacaoProcessados = ConcurrentHashMap.newKeySet();

    public void marcarConfirmado(String agendamentoId) {
        statusPorAgendamento.put(agendamentoId, STATUS_CONFIRMADO);
    }

    public void aplicarCompensacao(String eventoId, String agendamentoId) {

        if (!eventosCompensacaoProcessados.add(eventoId)) {
            return;
        }

        statusPorAgendamento.put(agendamentoId, STATUS_CANCELADO_POR_CONFLITO);
    }

    public String consultarStatus(String agendamentoId) {
        return statusPorAgendamento.getOrDefault(agendamentoId, STATUS_DESCONHECIDO);
    }
}