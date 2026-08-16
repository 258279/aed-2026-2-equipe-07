package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AgendamentoControllerTest {

    @Test
    void deveResponderAcceptedAoDispararConfirmacao() {

        AgendamentoService service = mock(AgendamentoService.class);

        AgendamentoController controller =
                new AgendamentoController(service);

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "SERV-004",
                        "2026-08-20T14:00:00-03:00",
                        "PADRAO",
                        "2026-08-16T12:30:00-03:00"
                );

        ResponseEntity<Void> resposta =
                controller.publicarConfirmacao(evento);

        assertEquals(
                HttpStatus.ACCEPTED,
                resposta.getStatusCode()
        );

        verify(service).publicarConfirmacao(evento);
    }

    @Test
    void deveReceberConfirmacaoPorHttpEResponderAccepted() throws Exception {

        AgendamentoService service = mock(AgendamentoService.class);

        AgendamentoController controller =
                new AgendamentoController(service);

        MockMvc mockMvc =
                MockMvcBuilders
                        .standaloneSetup(controller)
                        .build();

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
                ArgumentCaptor.forClass(
                        AgendamentoConfirmadoEvent.class
                );

        verify(service)
                .publicarConfirmacao(captor.capture());

        AgendamentoConfirmadoEvent eventoRecebido =
                captor.getValue();

        assertEquals(
                "EVT-001",
                eventoRecebido.getEventoId()
        );

        assertEquals(
                "AG-001",
                eventoRecebido.getAgendamentoId()
        );
    }
}