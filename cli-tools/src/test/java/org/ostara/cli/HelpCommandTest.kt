package org.ostara.cli

import org.checkerframework.checker.nullness.qual.AssertNonNullIfNonNull
import org.junit.Before
import org.junit.Test

class HelpCommandTest {

    lateinit var commands: List<Alias>
    var cliToolsApplication: CliToolsApplication = CliToolsApplication()

    @Before
    fun setUp() {
        commands = cliToolsApplication.init()

    }

    @Test
    fun testPrintHelp() {
        cliToolsApplication.printHelp(commands)
    }

    @Test
    fun testGetSubCommand() {
        val cmd = "createTopic"
        val subCommand = cliToolsApplication.getSubCommand(cmd, commands)
        AssertNonNullIfNonNull(subCommand.toString())
    }
}