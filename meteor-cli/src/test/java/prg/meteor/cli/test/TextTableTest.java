package prg.meteor.cli.test;

import java.util.ArrayList;
import java.util.List;
import org.meteor.cli.core.FormatPrint;

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

    static class Context {
        private int id;
        private String name;

        public Context(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
