package com.ofbizian.jcr.solr;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jcr.JcrConstants;
import org.apache.camel.component.solr.SolrConstants;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.camel.spring.SpringRouteBuilder;

import static org.apache.camel.builder.script.ScriptBuilder.script;

public class JcrSolrRoute extends SpringRouteBuilder {

    private static final String SOLR_URL = "SOLR_URL";

    private final Processor jcrSolrPropertyMapper = new Processor() {
        public void process(Exchange exchange) throws Exception {
            exchange.getIn().setHeader("SolrField.id", exchange.getIn().getBody());
            exchange.getIn().setHeader("SolrField.name", exchange.getProperty("name"));
            exchange.getIn().setHeader("SolrField.title", exchange.getProperty("title"));
        }
    };

    private final AggregationStrategy contentEnricher = new AggregationStrategy() {
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            return newExchange;
        }
    };

    public void configure() throws Exception {
        from("jcr://admin:admin@repository?deep=true&eventTypes=3")
                .split(body())
                .choice()
                .when(script("beanshell", "request.getBody().getType() == 1"))
                .to("direct:index")
                .when(script("beanshell", "request.getBody().getType() == 2"))
                .to("direct:delete")
                .otherwise()
                .log("Event type not recognized" + body().toString());

        from("direct:index")
                .enrich("direct:contentEnricher", contentEnricher)
                .process(jcrSolrPropertyMapper)

                .log("Indexing node with id: ${body}")
                .setHeader("SolrOperation", constant("INSERT"))
                .to(SOLR_URL)
                .setHeader("SolrOperation", constant("COMMIT"))
                .to(SOLR_URL);

        from("direct:contentEnricher")
                .setHeader(JcrConstants.JCR_OPERATION, constant(JcrConstants.JCR_GET_BY_ID))
                .setBody(script("beanshell", "request.getBody().getIdentifier()"))
                .log("Reading node with id: ${body}")
                .to("jcr://admin:admin@repository");

        from("direct:delete")
                .setHeader(SolrConstants.OPERATION, constant(SolrConstants.OPERATION_DELETE_BY_ID))
                .setBody(script("beanshell", "request.getBody().getIdentifier()"))

                .log("Deleting node with id: ${body}")
                .to(SOLR_URL)
                .setHeader("SolrOperation", constant("COMMIT"))
                .to(SOLR_URL);
    }
}
