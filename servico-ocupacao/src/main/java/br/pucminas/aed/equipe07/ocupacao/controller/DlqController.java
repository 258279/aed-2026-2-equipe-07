package br.pucminas.aed.equipe07.ocupacao.controller;

import br.pucminas.aed.equipe07.ocupacao.service.DlqReprocessamentoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/dlq")
public class DlqController {

    private static final Map<String, String> GRUPO_POR_CONSUMIDOR = Map.of(
            "ocupacao", "servico-ocupacao",
            "janela", "servico-ocupacao-agregacao-janelas"
    );

    private final DlqReprocessamentoService service;

    public DlqController(DlqReprocessamentoService service) {
        this.service = service;
    }

    @PostMapping("/{consumidor}/reprocessar")
    public ResponseEntity<Map<String, Integer>> reprocessar(@PathVariable String consumidor) {

        String groupId = GRUPO_POR_CONSUMIDOR.get(consumidor);

        if (groupId == null) {
            return ResponseEntity.badRequest().build();
        }

        int reprocessados = service.reprocessar(groupId);

        return ResponseEntity.ok(Map.of("reprocessados", reprocessados));
    }
}
