package org.meteor.expand.api;

import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;

public class DefaultConsumeQueue implements ConsumeQueue {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultConsumeQueue.class);

    @Override
    public long index() {
        return 0;
    }

    @Override
    public boolean updateIndex(long index) {
        return false;
    }
}
