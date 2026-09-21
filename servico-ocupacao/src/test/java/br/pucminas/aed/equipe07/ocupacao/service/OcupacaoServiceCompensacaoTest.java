package br.pucminas.aed.equipe07.ocupacao.service;

import br.pucminas.aed.equipe07.ocupacao.domain.AgendamentoConfirmadoEvent;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OcupacaoServiceCompensacaoTest {

    @Test
    void devePublicarCompensacaoQuandoDetectarConflitoDeHorario() {

        OcupacaoRepository repository = mock(OcupacaoRepository.class);
        CompensacaoService compensacaoService = mock(CompensacaoService.class);

        when(repository.eventoJaProcessado("EVT-300")).thenReturn(false);
        when(repository.existeConflitoDeHorario("PROF-300", "2026-09-20T10:00:00-03:00", "AG-300"))
                .thenReturn(true);

        OcupacaoService service = new OcupacaoService(repository, compensacaoService);

        AgendamentoConfirmadoEvent evento = new AgendamentoConfirmadoEvent(
                "EVT-300",
                "AG-300",
                "PROF-300",
                "2026-09-20T10:00:00-03:00"
        );

        service.processar(evento);

        verify(compensacaoService).publicarCancelamentoPorConflito(any());
        verify(repository).registrarEventoProcessado("EVT-300");
        verify(repository, never()).registrarOcupacao(any(), any(), any());
    }
}