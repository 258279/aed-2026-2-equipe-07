package br.pucminas.aed.equipe07.ocupacao.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AgendamentoConfirmadoJanelaRepository {

    private final JdbcTemplate banco;

    public AgendamentoConfirmadoJanelaRepository(JdbcTemplate banco) {
        this.banco = banco;
    }

    public int registrarConfirmacao(
            String janelaInicio,
            String janelaFim,
            String prioridade) {

        int linhasAtualizadas = banco.update(
                """
                UPDATE agregacao_confirmacoes_por_janela
                SET
                    janela_fim = ?,
                    quantidade_confirmacoes = quantidade_confirmacoes + 1,
                    atualizado_em = CURRENT_TIMESTAMP
                WHERE janela_inicio = ?
                  AND prioridade = ?
                """,
                janelaFim,
                janelaInicio,
                prioridade
        );

        if (linhasAtualizadas == 0) {
            banco.update(
                    """
                    INSERT INTO agregacao_confirmacoes_por_janela (
                        janela_inicio,
                        janela_fim,
                        prioridade,
                        quantidade_confirmacoes
                    )
                    VALUES (?, ?, ?, 1)
                    """,
                    janelaInicio,
                    janelaFim,
                    prioridade
            );
        }

        Integer quantidade = banco.queryForObject(
                """
                SELECT quantidade_confirmacoes
                FROM agregacao_confirmacoes_por_janela
                WHERE janela_inicio = ?
                  AND prioridade = ?
                """,
                Integer.class,
                janelaInicio,
                prioridade
        );

        return quantidade != null ? quantidade : 0;
    }
}