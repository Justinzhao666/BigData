package project.server.watch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * //TODO
 *
 * @author lucky.liu
 * @version 1.0.0
 * @email liuwb2010@gmail.com
 * @date 2016-03-18
 * Modification  History:
 * Date         Author        Version        Description
 * ------------------------------------------------------
 * 2016-03-18   lucky.liu     1.0.0          create
 */
public class RegexUtils {

    public static int[] findPosition(String text, String regex) {
        return findPosition(text, regex, 0, text == null ? 0 : text.length());
    }

    public static int[] findPosition(String text, String regex, int start, int end) {
        if (text == null) {
            return null;
        }
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(text);
        m.region(start, end);
        while (m.find()) {
            int len = m.groupCount();
            if (len > 0) {
                return new int[]{m.start(1), m.end(1)};
            }
        }
        return null;
    }

    /**
     * @param text  待匹配文本
     * @param regex 正则表达式
     * @return 返回第一个匹配结果
     */
    public static String findMatch(String text, String regex) {
        return findMatch(text, regex, 0, text == null ? 0 : text.length());
    }

    public static String findMatch(String text, String regex, int start, int end) {
        if (text == null) {
            return null;
        }
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(text);
        m.region(start, end);
        while (m.find()) {
            int len = m.groupCount();
            if (len > 0) {
                return m.group(1);
            }
        }
        return null;
    }
}