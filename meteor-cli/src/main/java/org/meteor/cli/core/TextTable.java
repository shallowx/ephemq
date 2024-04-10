package org.meteor.cli.core;

import java.util.Arrays;

public class TextTable {

    public final static String SEP = "\n";
    private String[] header;
    private String[][] rows;

    public TextTable(String[] header, String[][] rows) {
        this.header = header;
        this.rows = rows;
    }

    public String[] getHeader() {
        return header;
    }

    public void setHeader(String[] header) {
        this.header = header;
    }

    public String[][] getRows() {
        return rows;
    }

    public void setRows(String[][] rows) {
        this.rows = rows;
    }

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

    private void append(StringBuilder buff, String line) {
        buff.append(line).append(SEP);
    }

    private String buildSplitLine(int[] lens) {
        StringBuilder buff = new StringBuilder();
        buff.append("+");
        for (int len : lens) {
            buff.append(buildStrWithFixCharAndLength('-', len + 2));
            buff.append("+");
        }
        return buff.toString();
    }

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

    private String buildStrWithFixCharAndLength(char c, int len) {
        char[] cs = new char[len];
        Arrays.fill(cs, c);
        return new String(cs);
    }

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
