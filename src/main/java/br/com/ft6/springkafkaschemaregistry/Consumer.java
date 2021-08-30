package br.com.ft6.springkafkaschemaregistry;

import br.com.ft6.Customer;
import br.com.ft6.Operacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = "${app.topic}", groupId = "ft6-group")

    public void consumeOperacao(Operacao operacao, Acknowledgment acknowledgment){
        logger.info("\nReading message: Origem = {} | Entries = {}\n", operacao.getOperacaoOrigem(), operacao.getPayload().entrySet());
        acknowledgment.acknowledge();
    }

}
