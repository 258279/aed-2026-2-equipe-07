package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoCicloDeVidaService;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoService;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoStatusService;
import br.pucminas.aed.equipe07.agendamentos.service.ConflitoDeVersaoException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AgendamentoControllerTest {

    @Test
    void deveResponderAcceptedAoDispararConfirmacao() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001", "AG-001", "PROF-018", "SERV-004",
                        "2026-08-20T14:00:00-03:00", "PADRAO", "2026-08-16T12:30:00-03:00"
                );

        ResponseEntity<Void> resposta = controller.publicarConfirmacao(evento);

        assertEquals(HttpStatus.ACCEPTED, resposta.getStatusCode());
        verify(service).publicarConfirmacao(evento);
    }

    @Test
    void deveReceberConfirmacaoPorHttpEResponderAccepted() throws Exception {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        String json = """
                {
                    "eventoId": "EVT-001",
                    "agendamentoId": "AG-001",
                    "profissionalId": "PROF-018",
                    "servicoId": "SERV-004",
                    "inicioEm": "2026-08-20T14:00:00-03:00",
                    "prioridade": "PADRAO",
                    "ocorridoEm": "2026-08-16T12:30:00-03:00"
                }
                """;

        mockMvc.perform(
                        post("/agendamentos/confirmacoes")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(json)
                )
                .andExpect(status().isAccepted());

        ArgumentCaptor<AgendamentoConfirmadoEvent> captor =
                ArgumentCaptor.forClass(AgendamentoConfirmadoEvent.class);

        verify(service).publicarConfirmacao(captor.capture());

        assertEquals("EVT-001", captor.getValue().getEventoId());
        assertEquals("AG-001", captor.getValue().getAgendamentoId());
    }

    @Test
    void deveResponderOkAoCancelarComSucesso() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.cancelar("AG-001");

        assertEquals(HttpStatus.OK, resposta.getStatusCode());
        verify(cicloDeVidaService).cancelar("AG-001");
    }

    @Test
    void deveResponderConflictAoCancelarUmAgendamentoEmEstadoInvalido() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        doThrow(new IllegalStateException("já cancelado"))
                .when(cicloDeVidaService).cancelar("AG-002");

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.cancelar("AG-002");

        assertEquals(HttpStatus.CONFLICT, resposta.getStatusCode());
    }

    @Test
    void deveResponderConflictAoCancelarComConflitoDeVersao() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        doThrow(new ConflitoDeVersaoException("conflito", null))
                .when(cicloDeVidaService).cancelar("AG-003");

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.cancelar("AG-003");

        assertEquals(HttpStatus.CONFLICT, resposta.getStatusCode());
    }

    @Test
    void deveResponderOkAoMarcarNaoComparecimentoComSucesso() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.marcarNaoComparecimento("AG-004");

        assertEquals(HttpStatus.OK, resposta.getStatusCode());
        verify(cicloDeVidaService).marcarNaoComparecimento("AG-004");
    }

    @Test
    void deveConsultarStatusAtualDoAgendamento() throws Exception {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoStatusService statusService = mock(AgendamentoStatusService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        when(statusService.consultarStatus("AG-777")).thenReturn("CANCELADO_POR_CONFLITO");

        AgendamentoController controller =
                new AgendamentoController(service, statusService, cicloDeVidaService);

        MockMvc mockMvc =
                MockMvcBuilders
                        .standaloneSetup(controller)
                        .build();

        mockMvc.perform(get("/agendamentos/AG-777/status"))
                .andExpect(status().isOk())
                .andExpect(content().json("""
                        {"agendamentoId":"AG-777","status":"CANCELADO_POR_CONFLITO"}
                        """));
    }
}