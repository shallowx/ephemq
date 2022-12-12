package org.ostara.cli

import org.checkerframework.checker.nullness.qual.AssertNonNullIfNonNull
import org.junit.Test
import org.ostara.client.internal.Client

class ClientFactoryTest {
    @Test
    fun testClientStart() {
        val client: Client = ClientFactory.buildClient("127.0.0.1:9127")
        AssertNonNullIfNonNull(client.toString())
    }
}