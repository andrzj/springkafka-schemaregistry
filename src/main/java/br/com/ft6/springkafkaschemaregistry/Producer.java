package br.com.ft6.springkafkaschemaregistry;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.ft6.Operacao;

@RestController
@RequestMapping("/produce")
public class Producer {
    private final KafkaTemplate<String, Operacao> operacaoKafkaTemplate;

    public Producer(KafkaTemplate<String, Operacao> operacaoKafkaTemplate) {
        this.operacaoKafkaTemplate = operacaoKafkaTemplate;
    }

    @PostMapping(path = "/", consumes = "application/json", produces = "application/json")
    public ResponseEntity<Object> produce(
        @RequestHeader(name = "x-operacao_origem", required = true) String operacao_origem,
        @RequestHeader(name = "x-topico_destino", required = true) String topico_destino,
        @RequestBody Operacao operacao) 
        {

        operacao.setOperacaoOrigem(operacao_origem);
        operacao.setTopicoDestino(topico_destino);

        operacaoKafkaTemplate.send(operacao.getTopicoDestino().toString(), operacao);

        return ResponseEntity.ok("OK");
    }
}
