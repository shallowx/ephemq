package org.ostara.cli

import org.checkerframework.checker.nullness.qual.AssertNonNullIfNonNull
import org.junit.Before
import org.junit.Test

class HelpCommandTest {

    lateinit var commands: List<Alias>

    @Before
    fun setUp() {
        commands = init()

    }

    @Test
    fun testPrintHelp() {
        printHelp(commands)
    }

    @Test
    fun testGetSubCommand() {
        val cmd = "createTopic"
        val subCommand = getSubCommand(cmd, commands)
        AssertNonNullIfNonNull(subCommand.toString())
    }
}