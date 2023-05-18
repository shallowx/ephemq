package org.ostara.parser;

public interface ApplicationRunListener {

    ApplicationArguments startUp() throws Exception;

    ApplicationArguments argumentsPrepared(String[] args) throws Exception;

}
