package org.ostara.cli

import org.junit.Test
import org.ostara.cli.command.CreateTopicCommand
import org.ostara.cli.command.DeleteTopicCommand
import picocli.CommandLine

class TopicTest {

    val topic = "test"
    val host = "127.0.0.1:9127"

    @Test
    fun testCreateTopic() {
        val args = arrayOf("host", host, "-t", topic, "-p", "1", "-r", "1")
        CommandLine(CreateTopicCommand()).execute(*args)
    }

    @Test
    fun testDeleteTopic() {
        val args = arrayOf("host", host, "-t", topic)
        CommandLine(DeleteTopicCommand()).execute(*args)
    }
}