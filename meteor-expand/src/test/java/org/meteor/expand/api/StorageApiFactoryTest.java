package org.meteor.expand.api;

import org.junit.Assert;
import org.junit.Test;
import org.meteor.expand.bookkeeper.BookkeeperStorage;
import org.meteor.expand.core.ExpandConfig;
import org.meteor.expand.core.StorageType;
import org.meteor.expand.file.LocalFileStorage;

public class StorageApiFactoryTest {

    @Test
    public void testGetStorageApiFactory() throws Exception {
        StorageApiFactory instance = StorageApiFactory.getInstance(new ExpandConfig(null, null));
        StorageApi local = instance.getStorageApi(StorageType.LOCALE);
        Assert.assertTrue(local instanceof LocalFileStorage);
        StorageApi bookkeeper = instance.getStorageApi(StorageType.BOOKKEEPER);
        Assert.assertTrue(bookkeeper instanceof BookkeeperStorage);
    }
}
