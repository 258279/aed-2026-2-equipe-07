package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProjetorFaltasEstornos {

    private static final String TIPO_ESTORNO = "ESTORNO";
    private static final String TIPO_MULTA = "MULTA";

    private final JdbcTemplate banco;
    private final AgendamentoEventStore eventStore;

    public ProjetorFaltasEstornos(JdbcTemplate banco, AgendamentoEventStore eventStore) {
        this.banco = banco;
        this.eventStore = eventStore;
    }

    public void processar(List<EventoAgendamento> eventos) {

        for (EventoAgendamento evento : eventos) {

            String tipoProjecao = tipoDaProjecao(evento.getTipoEvento());

            if (tipoProjecao != null) {
                incrementar(dataDoEvento(evento), tipoProjecao);
            }
        }
    }

    public void reconstruirDoZero() {

        banco.update("DELETE FROM relatorio_faltas_estornos");

        processar(eventStore.carregarTodosOsEventos());
    }

    private String tipoDaProjecao(String tipoEvento) {

        if (Agendamento.TIPO_SINAL_ESTORNADO.equals(tipoEvento)) {
            return TIPO_ESTORNO;
        }

        if (Agendamento.TIPO_MULTA_COBRADA.equals(tipoEvento)) {
            return TIPO_MULTA;
        }

        return null;
    }

    private String dataDoEvento(EventoAgendamento evento) {
        return evento.getOcorridoEm().substring(0, 10);
    }

    private void incrementar(String data, String tipo) {

        int linhasAtualizadas = banco.update(
                """
                UPDATE relatorio_faltas_estornos
                SET quantidade = quantidade + 1
                WHERE data = ? AND tipo = ?
                """,
                data, tipo
        );

        if (linhasAtualizadas == 0) {
            banco.update(
                    """
                    INSERT INTO relatorio_faltas_estornos (data, tipo, quantidade)
                    VALUES (?, ?, 1)
                    """,
                    data, tipo
            );
        }
    }
}
