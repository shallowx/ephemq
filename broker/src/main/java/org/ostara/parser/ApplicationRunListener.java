package org.ostara.parser;

public interface ApplicationRunListener {

    ApplicationArguments starting() throws Exception;

    ApplicationArguments argumentsPrepared(String[] args) throws Exception;

}
