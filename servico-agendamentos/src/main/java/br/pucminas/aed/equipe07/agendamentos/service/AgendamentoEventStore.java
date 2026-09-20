package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public class AgendamentoEventStore {

    private final JdbcTemplate banco;

    public AgendamentoEventStore(JdbcTemplate banco) {
        this.banco = banco;
    }

    public List<EventoAgendamento> carregarStream(String agendamentoId) {

        return banco.query(
                """
                SELECT agendamento_id, versao, tipo_evento, payload, ocorrido_em
                FROM agendamento_eventos
                WHERE agendamento_id = ?
                ORDER BY versao
                """,
                mapeador(),
                agendamentoId
        );
    }

    public List<EventoAgendamento> carregarTodosOsEventos() {

        return banco.query(
                """
                SELECT agendamento_id, versao, tipo_evento, payload, ocorrido_em
                FROM agendamento_eventos
                ORDER BY agendamento_id, versao
                """,
                mapeador()
        );
    }

    @Transactional
    public void gravar(List<EventoAgendamento> eventos) {

        try {
            for (EventoAgendamento evento : eventos) {
                banco.update(
                        """
                        INSERT INTO agendamento_eventos (
                            agendamento_id, versao, tipo_evento, payload, ocorrido_em
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        evento.getAgendamentoId(),
                        evento.getVersao(),
                        evento.getTipoEvento(),
                        evento.getPayload(),
                        evento.getOcorridoEm()
                );
            }
        } catch (DataIntegrityViolationException e) {
            throw new ConflitoDeVersaoException(
                    "Conflito de versão ao gravar eventos do agendamento", e);
        }
    }

    private RowMapper<EventoAgendamento> mapeador() {
        return (rs, rowNum) -> new EventoAgendamento(
                rs.getString("agendamento_id"),
                rs.getInt("versao"),
                rs.getString("tipo_evento"),
                rs.getString("payload"),
                rs.getString("ocorrido_em")
        );
    }
}
