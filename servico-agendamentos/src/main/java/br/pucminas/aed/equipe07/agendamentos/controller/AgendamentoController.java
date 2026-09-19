package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoStatusService;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/agendamentos")
public class AgendamentoController {

    private final AgendamentoService service;
    private final AgendamentoStatusService statusService;

    public AgendamentoController(
            AgendamentoService service,
            AgendamentoStatusService statusService) {

        this.service = service;
        this.statusService = statusService;
    }

    @PostMapping("/confirmacoes")
    public ResponseEntity<Void> publicarConfirmacao(
            @RequestBody AgendamentoConfirmadoEvent evento) {

        service.publicarConfirmacao(evento);

        return ResponseEntity.accepted().build();
    }

    @GetMapping("/{agendamentoId}/status")
    public ResponseEntity<Map<String, String>> consultarStatus(
            @PathVariable String agendamentoId) {

        String status = statusService.consultarStatus(agendamentoId);

        return ResponseEntity.ok(Map.of(
                "agendamentoId", agendamentoId,
                "status", status
        ));
    }
}