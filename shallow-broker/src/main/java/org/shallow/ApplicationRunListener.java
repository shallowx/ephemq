package org.shallow;

public interface ApplicationRunListener {

    default ApplicationArguments starting() throws Exception {
        return null;
    }

    default ApplicationArguments argumentsPrepared(String[] args) throws Exception {
        return null;
    }

}
