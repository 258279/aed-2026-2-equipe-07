package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoCicloDeVidaService;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoService;
import br.pucminas.aed.equipe07.agendamentos.service.ConflitoDeVersaoException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/agendamentos")
public class AgendamentoController {

    private final AgendamentoService service;
    private final AgendamentoCicloDeVidaService cicloDeVidaService;

    public AgendamentoController(
            AgendamentoService service,
            AgendamentoCicloDeVidaService cicloDeVidaService) {

        this.service = service;
        this.cicloDeVidaService = cicloDeVidaService;
    }

    @PostMapping("/confirmacoes")
    public ResponseEntity<Void> publicarConfirmacao(
            @RequestBody AgendamentoConfirmadoEvent evento) {

        service.publicarConfirmacao(evento);

        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{agendamentoId}/cancelar")
    public ResponseEntity<Void> cancelar(@PathVariable String agendamentoId) {

        try {
            cicloDeVidaService.cancelar(agendamentoId);
            return ResponseEntity.ok().build();

        } catch (ConflitoDeVersaoException | IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @PostMapping("/{agendamentoId}/nao-comparecimento")
    public ResponseEntity<Void> marcarNaoComparecimento(@PathVariable String agendamentoId) {

        try {
            cicloDeVidaService.marcarNaoComparecimento(agendamentoId);
            return ResponseEntity.ok().build();

        } catch (ConflitoDeVersaoException | IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }
}
