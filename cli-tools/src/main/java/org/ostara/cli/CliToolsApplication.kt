package org.ostara.cli

import org.reflections.Reflections
import picocli.CommandLine
import picocli.CommandLine.Help
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    val application = CliToolsApplication()
    val commands = application.init()
    when (args.size) {
        0 -> {
            application.printHelp(commands)
            exitProcess(0)
        }

        1 -> {}

        2 -> {
            if (args[0] == "help") {
                val subCommand = application.getSubCommand(args[1], commands)
                if (subCommand == null) {
                    println("Not supported command, and command=" + args[1])
                    exitProcess(0)
                }
                
                CommandLine.usage(subCommand, System.out, Help.Ansi.AUTO)
                exitProcess(0)
            }
        }

        else -> {
            CommandLine(application.getSubCommand(args[0], commands)).execute(*args)
            exitProcess(0)
        }
    }
}

class CliToolsApplication {

    fun getSubCommand(cmd: String, commands: List<Alias>): Alias? {
        var alias: Alias? = null
        for (command in commands) {
            val annotation = command.javaClass.getAnnotation(CommandLine.Command::class.java)
            val name = annotation.name
            if (cmd == name) {
                alias = command
                break
            }
        }
        return alias
    }

    fun printHelp(commands: List<Alias>) {
        println("the most commonly use cli commands are:")
        for (command in commands) {
            val annotation = command.javaClass.getAnnotation(CommandLine.Command::class.java)
            if (annotation is CommandLine.Command) {
                val name = annotation.name
                val desc = annotation.description
                print(String.format("  %-20s %s%n", name, desc[0]))
            }
        }
    }

    fun init(): List<Alias> {
        val list: MutableList<Alias> = mutableListOf();

        val reflections = Reflections("org.ostara.cli")
        val clz = reflections.getSubTypesOf(Alias::class.javaObjectType)
        for (clazz in clz) {
            val newInstance = clazz.newInstance()
            list.add(newInstance)
        }
        return list
    }
}

