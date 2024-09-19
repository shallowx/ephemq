package org.meteor.cli.support;

import java.util.Arrays;

/**
 * The TextTable class provides a way to create and render text-based tables.
 * It includes methods to get and set headers and rows, and to convert the table
 * to a string representation.
 */
public class TextTable {

    /**
     * A constant string used as a separator in the TextTable class,
     * representing a newline character.
     */
    public final static String SEP = "\n";
    /**
     * The header array for a text table.
     * Each element of the array represents a column header in the table.
     */
    private String[] header;
    /**
     * A 2D array representing the rows of the text table.
     * Each row is an array of strings, where each string represents a cell in the table.
     */
    private String[][] rows;

    /**
     * Constructs a TextTable with the given header and rows.
     *
     * @param header The array of header strings for the table.
     * @param rows   The 2D array of row data strings for the table.
     */
    public TextTable(String[] header, String[][] rows) {
        this.header = header;
        this.rows = rows;
    }

    /**
     * Retrieves the header of the text table.
     *
     * @return an array of strings representing the header of the table
     */
    public String[] getHeader() {
        return header;
    }

    /**
     * Sets the header for the text table.
     *
     * @param header an array of strings representing the header of the table
     */
    public void setHeader(String[] header) {
        this.header = header;
    }

    /**
     * Retrieves the rows of the text table.
     *
     * @return a 2D array of Strings representing the rows of the table.
     */
    public String[][] getRows() {
        return rows;
    }

    /**
     * Sets the rows for the text table.
     *
     * @param rows A 2D array of strings representing the rows to be set in the table.
     */
    public void setRows(String[][] rows) {
        this.rows = rows;
    }

    /**
     * Returns a string representation of the TextTable object.
     *
     * @return A formatted string representing the table, complete with headers, rows, and split lines.
     */
    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();

        // calc length of every column
        int[] lens = calcLenOfColumn(header, rows);

        // build split line
        String splitLine = buildSplitLine(lens);

        // print split line
        append(buff, splitLine);

        // print header
        append(buff, buildArray(lens, header));

        // print split line
        append(buff, splitLine);

        for (String[] row : rows) {
            append(buff, buildArray(lens, row));
            append(buff, splitLine);
        }
        return buff.toString();
    }

    /**
     * Appends the given line and a separator to the provided StringBuilder.
     *
     * @param buff the StringBuilder to which the line will be appended
     * @param line the line to be appended to the StringBuilder
     */
    private void append(StringBuilder buff, String line) {
        buff.append(line).append(SEP);
    }

    /**
     * Builds a string that represents a horizontal split line in the text table.
     * Each segment of the split line corresponds to a column and is determined by the provided lengths.
     *
     * @param lens an array of integers representing the lengths of each column
     * @return a string representing the split line
     */
    private String buildSplitLine(int[] lens) {
        StringBuilder buff = new StringBuilder();
        buff.append("+");
        for (int len : lens) {
            buff.append(buildStrWithFixCharAndLength('-', len + 2));
            buff.append("+");
        }
        return buff.toString();
    }

    /**
     * Builds a formatted string representation of a row given the lengths of each column.
     *
     * @param lens An array of integers representing the lengths of each column.
     * @param row  An array of strings representing the values in the row.
     * @return A formatted string representing the row with proper spacing and delimiters.
     */
    private String buildArray(int[] lens, String[] row) {
        StringBuilder buff = new StringBuilder();
        buff.append("|");
        for (int i = 0; i < header.length; i++) {
            String s = null;
            if (i < row.length) {
                s = row[i];
            }
            buff.append(" ");
            buff.append(polishing(s, lens[i]));
            buff.append(" |");
        }
        return buff.toString();
    }

    /**
     * Calculates the maximum width of each column in a table based on the provided headers and row data.
     *
     * @param header an array of strings representing the headers of the table columns
     * @param rows a 2D array of strings representing the rows of the table
     * @return an array of integers where each element represents the maximum width of the corresponding column
     */
    private int[] calcLenOfColumn(String[] header, String[][] rows) {
        int[] lens = new int[header.length];
        Arrays.fill(lens, 0);
        for (int i = 0; i < header.length; i++) {
            lens[i] = Math.max(stringLen(header[i]), lens[i]);
        }
        for (String[] row : rows) {
            for (int i = 0; i < header.length; i++) {
                if (i < row.length) {
                    lens[i] = Math.max(stringLen(row[i]), lens[i]);
                }
            }
        }
        return lens;
    }

    /**
     * Calculates the length of the given string `s` taking into account special character ranges
     * where characters are considered to be of varying widths.
     *
     * @param s The input string whose length is to be calculated.
     * @return The calculated length of the string. If `s` is null, the method returns 0.
     */
    public static int stringLen(String s) {
        if (s == null) {
            return 0;
        }
        int len = 0;
        String ch = "[\u0391-\uFFE5]";
        String ch2 = "[\u00B7]";
        for (int i = 0; i < s.length(); i++) {
            String tmp = s.substring(i, i + 1);
            if (tmp.matches(ch) || tmp.matches(ch2)) {
                len += 2;
            } else {
                len += 1;
            }
        }
        return len;
    }

    /**
     * Constructs a string consisting of a specified character repeated to a specified length.
     *
     * @param c the character to repeat
     * @param len the length of the resulting string
     * @return a string consisting of the character {@code c} repeated {@code len} times
     */
    private String buildStrWithFixCharAndLength(char c, int len) {
        char[] cs = new char[len];
        Arrays.fill(cs, c);
        return new String(cs);
    }

    /**
     * Adjusts the input string to match the specified length by either padding with spaces or truncating with ellipses.
     *
     * @param str The input string to be polished. If null, a string of spaces of length `destLength` will be returned.
     * @param destLength The desired length of the resulting string.
     * @return The polished string that matches the desired length. If the input string length is less than `destLength`, it will be padded with spaces.
     *         If greater, it will be truncated and end with ellipses followed by spaces (if needed) to reach `destLength`.
     */
    private String polishing(String str, int destLength) {
        if (str == null) {
            return buildStrWithFixCharAndLength(' ', destLength);
        }
        int len = stringLen(str);
        if (len < destLength) {
            int dif = destLength - len;
            char[] cs = new char[dif];
            Arrays.fill(cs, ' ');
            return str + new String(cs);
        } else if (len == destLength) {
            return str;
        } else {
            StringBuilder sb = new StringBuilder();
            char[] cs = str.toCharArray();
            for (char c : cs) {
                if (stringLen(sb.toString() + c) >= destLength - 3) {
                    break;
                }
                sb.append(c);
            }
            String s = sb + "...";
            len = stringLen(s);
            if (len < destLength) {
                int dif = destLength - len;
                cs = new char[dif];
                Arrays.fill(cs, ' ');
                return s + new String(cs);
            }
            return s;
        }
    }
}
