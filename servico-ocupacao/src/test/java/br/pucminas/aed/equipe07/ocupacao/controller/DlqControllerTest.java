package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.service.DlqReprocessamentoService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DlqControllerTest {

    @Test
    void deveReprocessarConsumidorOcupacaoEResponderQuantidade() {

        DlqReprocessamentoService service = mock(DlqReprocessamentoService.class);
        when(service.reprocessar("servico-ocupacao")).thenReturn(2);

        DlqController controller = new DlqController(service);

        ResponseEntity<Map<String, Integer>> resposta = controller.reprocessar("ocupacao");

        assertEquals(HttpStatus.OK, resposta.getStatusCode());
        assertEquals(2, resposta.getBody().get("reprocessados"));
    }

    @Test
    void deveReprocessarConsumidorJanelaEResponderQuantidade() {

        DlqReprocessamentoService service = mock(DlqReprocessamentoService.class);
        when(service.reprocessar("servico-ocupacao-agregacao-janelas")).thenReturn(0);

        DlqController controller = new DlqController(service);

        ResponseEntity<Map<String, Integer>> resposta = controller.reprocessar("janela");

        assertEquals(HttpStatus.OK, resposta.getStatusCode());
        assertEquals(0, resposta.getBody().get("reprocessados"));
    }

    @Test
    void deveResponderBadRequestParaConsumidorDesconhecido() {

        DlqReprocessamentoService service = mock(DlqReprocessamentoService.class);
        DlqController controller = new DlqController(service);

        ResponseEntity<Map<String, Integer>> resposta = controller.reprocessar("inexistente");

        assertEquals(HttpStatus.BAD_REQUEST, resposta.getStatusCode());
    }
}
