package org.ephemq.cli.test;

import java.util.ArrayList;
import java.util.List;
import org.ephemq.cli.support.FormatPrint;

/**
 * Demonstrates the functionality of formatting and printing a list of context objects
 * to a tabular text representation.
 */
public class TextTableTest {

    /**
     * +----+------+
     * | id | name |
     * +----+------+
     * | 1  | A    |
     * +----+------+
     * | 2  | B    |
     * +----+------+
     */
    public static void main(String[] args) {
        String[] title = {"id", "name"};
        List<Context> contexts = new ArrayList<Context>();
        contexts.add(new Context(1, "A"));
        contexts.add(new Context(2, "B"));
        FormatPrint.formatPrint(contexts, title);
    }

    /**
     * Represents a contextual object with an identifier and a name.
     */
    static class Context {
        /**
         * The unique identifier for a Context object.
         */
        private int id;
        /**
         * The name of the context, which provides a human-readable identifier for the context object.
         */
        private String name;

        /**
         * Constructs a new Context with the specified id and name.
         *
         * @param id   the unique identifier for the context
         * @param name the name of the context
         */
        public Context(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Returns the identifier of this context.
         *
         * @return the id of this context
         */
        public int getId() {
            return id;
        }

        /**
         * Retrieves the name associated with this context.
         *
         * @return the name of the context.
         */
        public String getName() {
            return name;
        }
    }
}
