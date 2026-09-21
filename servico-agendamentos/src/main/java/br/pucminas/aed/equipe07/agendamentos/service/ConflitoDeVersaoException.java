package br.pucminas.aed.equipe07.agendamentos.service;

public class ConflitoDeVersaoException extends RuntimeException {

    public ConflitoDeVersaoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
