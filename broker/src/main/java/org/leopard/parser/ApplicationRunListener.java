package org.leopard.parser;

public interface ApplicationRunListener {

    ApplicationArguments starting() throws Exception;

    ApplicationArguments argumentsPrepared(String[] args) throws Exception;

}
