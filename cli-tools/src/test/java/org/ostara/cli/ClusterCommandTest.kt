package org.ostara.cli

import org.junit.Test
import org.ostara.cli.command.ClusterCommand
import picocli.CommandLine

class ClusterCommandTest {

    @Test
    fun testQueryNodeInfo() {
        val args = arrayOf("host", "127.0.0.1:9127")
        CommandLine(ClusterCommand()).execute(*args)
    }
}
