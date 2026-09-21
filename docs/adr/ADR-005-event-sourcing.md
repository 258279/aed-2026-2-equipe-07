# ADR-005 — Event Sourcing no agregado Agendamento

## Status

Aceito

## Contexto

O ADR-002 já identificou, como parte do domínio, um caminho de exceção com compensação
que nunca foi implementado em código: cancelamento dentro do prazo devolve o sinal
pago; não comparecimento (no-show) gera cobrança de multa. Esse caminho precisa de
rastreabilidade real — não basta saber o estado final ("cancelado"), é preciso saber
a sequência exata de fatos que levou a ele, para auditoria e para responder perguntas
sobre o passado que podem mudar de interpretação com o tempo (ex.: uma disputa sobre
se o cancelamento aconteceu dentro do prazo).

## Decisão

Aplicar Event Sourcing ao agregado `Agendamento`, especificamente ao trecho do seu
ciclo de vida que vai de `Confirmado` até `Cancelado + SinalEstornado` ou
`NaoCompareceu + MultaCobrada`. O estado do agregado deixa de ser gravado diretamente
e passa a ser reconstruído por fold da sequência de eventos gravados em um Event Store
próprio (`agendamento_eventos`), append-only, ordenado e versionado por stream
(`agendamento_id`).

Nenhum outro agregado do sistema (a projeção de ocupação, o agregador por janela) é
afetado — Event Sourcing é aplicado só aqui, de propósito.

## Alternativas consideradas

- **CRUD tradicional (UPDATE no estado do agendamento)**: descartado porque perde a
  sequência de fatos — depois de um UPDATE, não há como reconstruir "o que aconteceu
  e quando", só o estado final. Não sustenta auditoria nem disputa sobre prazo.
- **Log de auditoria separado, ao lado de uma tabela de estado tradicional**:
  melhora a rastreabilidade, mas cria duas fontes de verdade que podem divergir (o log
  diz uma coisa, o estado gravado diz outra, e nada força consistência entre os dois).
  Event Sourcing elimina essa divergência ao fazer do log a única fonte de verdade.

## Consequências aceitas

- **Evolução de esquema (upcasting)**: se o formato de um evento precisar mudar no
  futuro (ex.: adicionar um campo em `SinalEstornado`), eventos antigos já gravados
  continuam no formato velho para sempre — o código que faz fold precisa saber ler
  as duas versões, ou um processo de upcasting precisa traduzir o formato antigo pro
  novo na leitura.
- **Crescimento do log**: o Event Store só cresce, nunca encolhe. Para o volume deste
  domínio (no máximo 3 eventos por agendamento) isso não é um problema agora, mas em
  escala maior exigiria arquivamento de streams antigos.
- **LGPD (direito à eliminação)**: um log append-only não pode simplesmente apagar o
  dado pessoal de um cliente sob pedido, sem quebrar a integridade do histórico. A
  saída aceita é **crypto-shredding**: dado pessoal sensível no payload seria
  criptografado com uma chave por cliente; "esquecer" o cliente significa jogar fora a
  chave, não o evento — o evento continua existindo, mas ilegível.
- **Curva de aprendizado da equipe**: reconstruir estado por fold em vez de ler um
  campo direto do banco é um modelo mental novo, mais custoso de debugar (é preciso
  reproduzir a sequência de eventos pra entender um estado, não só olhar uma linha de
  tabela). Aceito porque só um agregado pequeno usa esse modelo — o resto do sistema
  continua CRUD simples, o que os revisores confirmaram ser um acerto de escopo, não
  dívida técnica.
