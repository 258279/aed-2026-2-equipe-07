package br.pucminas.aed.equipe07.ocupacao.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OcupacaoRepository {

    private final JdbcTemplate banco;

    public OcupacaoRepository(JdbcTemplate banco) {
        this.banco = banco;
    }

    public boolean eventoJaProcessado(String eventoId) {

        Integer quantidade = banco.queryForObject(
                """
                SELECT COUNT(*)
                FROM eventos_processados
                WHERE evento_id = ?
                """,
                Integer.class,
                eventoId
        );

        return quantidade != null && quantidade > 0;
    }

    public void registrarEventoProcessado(String eventoId) {

        banco.update(
                """
                INSERT INTO eventos_processados (evento_id)
                VALUES (?)
                """,
                eventoId
        );
    }

    public void registrarOcupacao(
            String agendamentoId,
            String profissionalId,
            String inicioEm) {

        banco.update(
                """
                INSERT INTO projecao_ocupacao (
                    agendamento_id,
                    profissional_id,
                    inicio_em
                )
                VALUES (?, ?, ?)
                """,
                agendamentoId,
                profissionalId,
                inicioEm
        );
    }
}